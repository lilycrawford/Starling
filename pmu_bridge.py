# -*- coding: utf-8 -*-
"""
pmu_bridge.py

Bridges MQTT Sampled Value (SV) data into connected WebSocket clients.

Architecture:

    Mosquitto (MQTT)
        -> mqttSVsub.Subscriber.receive()   [background thread, blocking]
        -> queue.Queue                       [thread-safe handoff]
        -> asyncio broadcast_loop()          [runs in FastAPI's event loop]
        -> WebSocket clients (browser)

This mirrors the threading pattern already used in SVtoWave.py: a dedicated
thread does the blocking MQTT receive() work and pushes frames onto a queue;
a separate loop drains the queue and does something with each frame (here:
broadcast over WebSocket, instead of writing to a FLAC file).
"""

import asyncio
import base64
import json
import os
import queue
import sys
import threading
import time
from fastapi import WebSocket

# find mqttSVsub in subfolder OpenPMUcode -> SVtoWave -> mqttSVsub.py
_SVTOWAVE_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "OpenPMU code", "SVtoWave"
)
if _SVTOWAVE_DIR not in sys.path:
    sys.path.insert(0, _SVTOWAVE_DIR)
 
try:
    import mqttSVsub
    print("Imported mqttSVsub")
except:
    print("Could not import mqttSVsub")
    


class PMUBridge:
    def __init__(self, config: dict):
        self.config = config
        self.frame_queue: "queue.Queue" = queue.Queue(maxsize=500)

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._subscriber: mqttSVsub.Subscriber | None = None

        self._clients: set[WebSocket] = set()
        self._clients_lock = asyncio.Lock()

        self.stats = {
            "frames_received": 0,
            "frames_broadcast": 0,
            "frames_dropped": 0,
            "clients_connected": 0,
            "last_frame_time": None,
            "started_at": None,
            "connected_to_broker": False,
        }

    # ================= Background thread: MQTT -> queue =================

    def _subscriber_thread(self):
        """
        Mirrors SVtoWave.get_PMU(): connect, loop calling receive(), push each
        frame onto the queue. If the broker connection dies, retries rather
        than killing the thread silently.
        """
        while not self._stop_event.is_set():
            try:
                self._subscriber = mqttSVsub.Subscriber(self.config)
                self.stats["connected_to_broker"] = True
            except Exception as e:
                self.stats["connected_to_broker"] = False
                print(f"[PMUBridge] Failed to connect to MQTT broker: {e}")
                print("[PMUBridge] Retrying in 3s...")
                time.sleep(3)
                continue

            print("[PMUBridge] Connected. Listening for SV frames...")

            while not self._stop_event.is_set():
                try:
                    dataInfo = self._subscriber.receive(timeout=1)
                except Exception:
                    # queue.Empty from the internal MQTT queue during quiet
                    # periods lands here - this is normal, keep polling.
                    continue

                if dataInfo is None:
                    continue

                self.stats["frames_received"] += 1
                self.stats["last_frame_time"] = time.time()

                try:
                    self.frame_queue.put_nowait(dataInfo)
                except queue.Full:
                    # Consumer can't keep up. Drop the oldest frame rather than
                    # blocking the receiver - staying live matters more than
                    # replaying every historical frame.
                    try:
                        self.frame_queue.get_nowait()
                        self.stats["frames_dropped"] += 1
                    except queue.Empty:
                        pass
                    self.frame_queue.put_nowait(dataInfo)

            self._subscriber.close()
            self.stats["connected_to_broker"] = False

    def start(self):
        self._stop_event.clear()
        self.stats["started_at"] = time.time()
        self._thread = threading.Thread(target=self._subscriber_thread, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        if self._subscriber is not None:
            self._subscriber.close()

    # ================= WebSocket client management =================

    async def register(self, ws: WebSocket):
        await ws.accept()
        async with self._clients_lock:
            self._clients.add(ws)
            self.stats["clients_connected"] = len(self._clients)
        print(f"[PMUBridge] Client connected ({self.stats['clients_connected']} total)")

    async def unregister(self, ws: WebSocket):
        async with self._clients_lock:
            self._clients.discard(ws)
            self.stats["clients_connected"] = len(self._clients)
        print(f"[PMUBridge] Client disconnected ({self.stats['clients_connected']} total)")

    # ================= Frame -> wire format =================

    @staticmethod
    def _frame_to_message(dataInfo: dict) -> str:
        """
        Converts a parsed SV frame (from mqttSVsub.Subscriber.receive()) into
        a compact JSON message for the browser.

        We forward the ALREADY base64-encoded payload rather than decoded
        numpy arrays:
          1. mqttSVsub already computes PayloadBase64 - re-encoding a numpy
             array into a JSON list of numbers is slower to produce and much
             bigger on the wire than one base64 string.
          2. The browser needs typed arrays for a real-time chart anyway, and
             base64 -> ArrayBuffer -> Int16Array is cheap in JS.

        Message shape:
        {
            "type": "frame",
            "frame": <int>,
            "fs": <int>,
            "n": <int>,
            "bits": <int>,
            "channels": <int>,
            "date": "YYYY-MM-DD",
            "time": "HH:MM:SS.ffffff",
            "data": {
                "Channel_0": {"name": ..., "phase": ..., "payload": "<base64, int16 big-endian>"},
                ...
            }
        }
        """
        channels_out = {}
        for i in range(dataInfo.get("Channels", 0)):
            key = f"Channel_{i}"
            ch = dataInfo.get(key)
            if ch is None:
                continue

            payload_b64 = ch.get("PayloadBase64")
            if payload_b64 is None and "PayloadRAW" in ch:
                # XML-sourced frames (PMU.py path) don't preserve the base64
                # string separately - re-encode from the raw int16 array.
                payload_b64 = base64.b64encode(ch["PayloadRAW"].tobytes()).decode("ascii")

            channels_out[key] = {
                "name": ch.get("Name"),
                "phase": ch.get("Phase"),
                "payload": payload_b64,
            }

        message = {
            "type": "frame",
            "frame": dataInfo.get("Frame"),
            "fs": dataInfo.get("Fs"),
            "n": dataInfo.get("n"),
            "bits": dataInfo.get("bits"),
            "channels": dataInfo.get("Channels"),
            "date": dataInfo.get("Date"),
            "time": dataInfo.get("Time"),
            "data": channels_out,
        }
        return json.dumps(message)

    # ================= Main loop: queue -> broadcast =================

    async def broadcast_loop(self):
        """
        Runs as an asyncio task in FastAPI's event loop. Drains the
        thread-fed queue.Queue WITHOUT blocking the event loop (via
        run_in_executor), and broadcasts each frame to all connected clients.
        """
        loop = asyncio.get_running_loop()

        while not self._stop_event.is_set():
            dataInfo = await loop.run_in_executor(None, self._blocking_get)

            if dataInfo is None:
                continue

            message = self._frame_to_message(dataInfo)
            await self._broadcast(message)
            self.stats["frames_broadcast"] += 1

    def _blocking_get(self):
        try:
            return self.frame_queue.get(timeout=1)
        except queue.Empty:
            return None

    async def _broadcast(self, message: str):
        async with self._clients_lock:
            clients = list(self._clients)

        dead = []
        for ws in clients:
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)

        if dead:
            async with self._clients_lock:
                for ws in dead:
                    self._clients.discard(ws)
                self.stats["clients_connected"] = len(self._clients)
