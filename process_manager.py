# -*- coding: utf-8 -*-
"""
process_manager.py

Launches and supervises external Python scripts (WaveMU.py, ADCtoMQTT.py)
as child processes of the dashboard server, so starting server.py / launch.bat
brings up the whole pipeline instead of requiring separate terminals.

Each process is launched with its OWN script directory as the working
directory - this matters because both WaveMU.py and ADCtoMQTT.py load
files using relative paths (a FLAC file, a local config.json), which only
resolve correctly if the process's cwd is set to where the script lives.
"""

import os
import sys
import subprocess
import time


class ExternalProcess:
    def __init__(self, name: str, script_path: str, delay: float = 0.0):
        self.name = name
        self.script_path = os.path.abspath(script_path)
        self.cwd = os.path.dirname(self.script_path)
        self.delay = delay
        self.proc: "subprocess.Popen | None" = None

    def start(self):
        if self.is_running():
            print(f"[ProcessManager] '{self.name}' already running (pid {self.proc.pid}) - skipping")
            return

        if not os.path.exists(self.script_path):
            print(f"[ProcessManager] WARNING: '{self.name}' script not found at "
                  f"{self.script_path} - skipping. Check the path in config.json.")
            return

        if self.delay:
            time.sleep(self.delay)

        print(f"[ProcessManager] Starting {self.name} ({self.script_path})")

        # On Windows, give each process its own console window so its print()
        # / progress-dot output is visible exactly like running it manually.
        creationflags = subprocess.CREATE_NEW_CONSOLE if os.name == "nt" else 0

        self.proc = subprocess.Popen(
            [sys.executable, self.script_path],
            cwd=self.cwd,
            creationflags=creationflags,
        )

    def stop(self):
        if self.proc is None:
            return
        if self.proc.poll() is None:  # still running
            print(f"[ProcessManager] Stopping {self.name} (pid {self.proc.pid})")
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                print(f"[ProcessManager] {self.name} didn't exit in time - killing")
                self.proc.kill()
        self.proc = None

    def is_running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None


class ProcessManager:
    def __init__(self, process_configs: list):
        """
        process_configs: list of dicts, e.g.
            [{"name": "ADCtoMQTT", "script": "D:/.../ADCtoMQTT.py", "delay": 0},
             {"name": "WaveMU",    "script": "D:/.../WaveMU.py",    "delay": 2}]
        """
        self.processes = [
            ExternalProcess(cfg["name"], cfg["script"], cfg.get("delay", 0.0))
            for cfg in process_configs
        ]

    def start_all(self):
        """
        Starts processes IN ORDER with their configured delays. Order matters
        here: ADCtoMQTT should have its UDP socket bound before WaveMU starts
        firing packets at it, since UDP silently drops anything sent before
        the receiver is listening.

        This method blocks for the sum of all delays - call it via
        loop.run_in_executor() from an async context so it doesn't stall
        the FastAPI event loop.
        """
        for p in self.processes:
            p.start()

    def stop_all(self):
        for p in self.processes:
            p.stop()

    def status(self) -> dict:
        return {p.name: p.is_running() for p in self.processes}
