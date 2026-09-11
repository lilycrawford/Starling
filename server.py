# =-=-=-=-= IMPORTS =-=-=-=-=
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
import os
import sys
import json
import signal
import asyncio
from collections import deque
import math
import time
import datetime
import socket
import threading
import queue
import subprocess

from pmu_bridge import PMUBridge
from process_manager import ProcessManager

# Enable CORS so local HTML file dashboard can talk to server safely
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

script_dir = os.path.dirname(os.path.abspath(__file__))

# Mount static asset folders safely
for folder in ["style", "js", "assets", "vendor"]:
    target = os.path.join(script_dir, folder)
    if os.path.exists(target):
        app.mount(f"/{folder}", StaticFiles(directory=target), name=folder)

# =-=-=-=-= PMU MQTT -> WEBSOCKET BRIDGE =-=-=-=-=
# Load the config file (or fall back to hardcoded defaults)
def loadConfig(configFile="config.json"):
    print("- 'loadConfig' function triggered")
    try:
        with open(configFile, 'r') as jsonFile:
            print("- Sucessfully loaded original config.json file.")
            return json.load(jsonFile)
    except (FileNotFoundError, json.JSONDecodeError):
        print(f"Warning: {configFile} not found or invalid. Using fallback configuration.")
        return {    
            "wavePath": "D:/OpenPMU code/WaveLogs/", # "C:/mnt/OpenPMU-usb0/WaveLogs/"
            "mqttBroker": "127.0.0.1",
            "mqttPort": 1883,
            "mqttTopicSV": "OpenPMU/ADC_SV",
            "recMask": [0,1],
            "allowDeletion": "True",
            "daysToKeep": 7
        } 

print("- Running pmu_config declaration.")
pmu_config = loadConfig("config.json")

bridge = PMUBridge(pmu_config)
_broadcast_task: asyncio.Task | None = None

# =-=-=-=-= EXTERNAL PROCESSES (WaveMU, ADCtoMQTT, etc.) =-=-=-=-=
process_manager = ProcessManager(pmu_config.get("externalProcesses", []))


@app.on_event("startup")
async def start_external_processes():
    loop = asyncio.get_running_loop()
    # start_all() uses time.sleep() for its staggered delays - run it in a
    # worker thread so it doesn't block the FastAPI event loop.
    await loop.run_in_executor(None, process_manager.start_all)
    print("[server] External processes launched:", process_manager.status())


@app.on_event("shutdown")
async def stop_external_processes():
    process_manager.stop_all()


@app.on_event("startup")
async def start_pmu_bridge():
    global _broadcast_task
    bridge.start()                                   # background thread: MQTT -> queue
    _broadcast_task = asyncio.create_task(bridge.broadcast_loop())  # queue -> websockets
    print("[server] PMU bridge started")


@app.on_event("shutdown")
async def stop_pmu_bridge():
    if _broadcast_task is not None:
        _broadcast_task.cancel()
    bridge.stop()
    print("[server] PMU bridge stopped")


@app.websocket("/ws/sv")
async def websocket_sv(websocket: WebSocket):
    """
    Browser connects here to receive a live stream of SV frames as JSON
    messages (see PMUBridge._frame_to_message for the schema).
    """
    await bridge.register(websocket)
    try:
        while True:
            # We don't expect the client to send anything meaningful, but we
            # need to await something to detect disconnects promptly.
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await bridge.unregister(websocket)


@app.post("/api/start-pipeline")
async def start_pipeline():
    """
    Manually (re)starts WaveMU / ADCtoMQTT. Safe to call repeatedly - any
    process already running is left alone rather than being duplicated.
    """
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, process_manager.start_all)
    return JSONResponse({"status": "ok", "processes": process_manager.status()})


@app.get("/api/status")
def get_status():
    """Quick health/debug endpoint - is the bridge connected, how many frames, etc."""
    return JSONResponse({
        **bridge.stats,
        "external_processes": process_manager.status(),
    })


# for the 'stop server' button
@app.post("/api/shutdown")
def shutdown_server():
    os.kill(os.getpid(), signal.SIGINT)
    return {"message": "Server shutting down..."}


@app.get("/")
@app.get("/index.html")
def get_dashboard():
    return FileResponse(os.path.join(script_dir, "index.html"))


# =-=-=-=-= MAIN =-=-=-=-=
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 55555))
    uvicorn.run(app, host="127.0.0.1", port=port)
