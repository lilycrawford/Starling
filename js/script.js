// script.js
//
// Connects to the backend's /ws/sv WebSocket, decodes incoming SV frames,
// keeps a rolling per-channel sample buffer, and drives a uPlot chart that
// looks like a continuously scrolling oscilloscope trace.

// =-=-=-=-= CONFIG =-=-=-=-=

const WS_PATH = "/ws/sv";
const WINDOW_SECONDS = 0.25;
const RECONNECT_DELAY_MS = 100;

const ADC_MAX_VALUE = 32767;
const ADC_RANGE = 5.0;

// Chart redraws happen every animation frame now (no artificial throttle) -
// the chart always shows the newest buffered data.

// Parses OpenPMU's "Date"/"Time" fields (e.g. "2022-02-20" + "16:00:03.450")
// into a Unix-epoch millisecond timestamp. Uses Date.UTC deliberately: these
// are synthetic recording timestamps embedded in the data, not real wall-clock
// time, so treating them as UTC keeps the math simple and avoids the
// browser's local timezone silently shifting them.
function parseFrameTimestamp(dateStr, timeStr) {
    if (!dateStr || !timeStr) return null;
    const [y, mo, d] = dateStr.split("-").map(Number);
    const timeParts = timeStr.split(":");
    const hh = Number(timeParts[0]);
    const mm = Number(timeParts[1]);
    const [ssStr, fracStr] = timeParts[2].split(".");
    const ss = Number(ssStr);
    const ms = fracStr ? Math.round(Number("0." + fracStr) * 1000) : 0;
    return Date.UTC(y, mo - 1, d, hh, mm, ss, ms);
}

// Formats a millisecond timestamp as full ISO 8601 (the xkcd-approved format).
function formatISO8601(ms) {
    if (ms === null || ms === undefined || Number.isNaN(ms)) return "--";
    return new Date(ms).toISOString();
}

const CHANNEL_COLORS = ["#BA3636", "#C2955A", "#6B3636", "#BB6B36", "#1B3A34", "#933636", "#E7C8B6", "#261410"];

class RollingBuffer {
    constructor(capacity) {
        this.capacity = capacity;
        this.data = new Float32Array(capacity);
        this.length = 0;
    }

    push(samples) {
        const n = samples.length;

        if (n >= this.capacity) {
            this.data.set(samples.subarray(n - this.capacity));
            this.length = this.capacity;
            return;
        }

        if (this.length + n <= this.capacity) {
            this.data.set(samples, this.length);
            this.length += n;
        } else {
            const overflow = this.length + n - this.capacity;
            this.data.copyWithin(0, overflow, this.length);
            this.length -= overflow;
            this.data.set(samples, this.length);
            this.length += n;
        }
    }

    view() {
        return this.data.subarray(0, this.length);
    }
}

const state = {
    ws: null,
    connected: false,
    paused: false,
    fs: null,
    capacity: null,
    xAxis: null,
    buffers: new Map(),
    channelOrder: [],
    hiddenChannels: new Set(),
    framesThisSecond: 0,
    lastFrameRateSample: performance.now(),
    currentFrameRate: 0,
    uplot: null,
    lastRenderTime: 0,
    rafCallCount: 0,
    redrawCount: 0,
    currentRafRate: 0,
    currentRedrawRate: 0,
    latestTimestampMs: null,   // end-of-most-recent-frame, from the data itself (ms epoch)
    latestTimestampSec: null,  // same, in seconds (uPlot's time scale wants seconds)
    xAbsScratch: null,         // reused Float64Array of absolute x-values, avoids per-frame allocation
};

function connectWebSocket() {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const url = `${protocol}//${window.location.host}${WS_PATH}`;

    const ws = new WebSocket(url);
    state.ws = ws;

    ws.onopen = () => {
        state.connected = true;
        updateConnectionStatus();
    };

    ws.onmessage = (event) => {
        handleFrameMessage(event.data);
    };

    ws.onclose = () => {
        state.connected = false;
        updateConnectionStatus();
        setTimeout(connectWebSocket, RECONNECT_DELAY_MS);
    };

    ws.onerror = () => {
        ws.close();
    };
}

function updateConnectionStatus() {
    const dot = document.getElementById("conn-dot");
    const label = document.getElementById("conn-label");
    if (!dot || !label) return;

    dot.classList.toggle("conn-dot-live", state.connected);
    dot.classList.toggle("conn-dot-down", !state.connected);
    label.textContent = state.connected ? "Live" : "Reconnecting...";
}

function decodeBase64ToInt16BE(base64) {
    const raw = atob(base64);
    const bytes = new Uint8Array(raw.length);
    for (let i = 0; i < raw.length; i++) bytes[i] = raw.charCodeAt(i);

    const view = new DataView(bytes.buffer);
    const n = bytes.length / 2;
    const out = new Float32Array(n);
    for (let i = 0; i < n; i++) {
        const raw16 = view.getInt16(i * 2, false);
        out[i] = (raw16 / ADC_MAX_VALUE) * ADC_RANGE;
    }
    return out;
}

function ensureBuffersSized(fs) {
    if (state.fs === fs && state.capacity !== null) return;

    state.fs = fs;
    state.capacity = Math.max(8, Math.round(fs * WINDOW_SECONDS));

    state.xAxis = new Float64Array(state.capacity);
    for (let i = 0; i < state.capacity; i++) {
        state.xAxis[i] = (i - state.capacity) / fs;
    }
    state.xAbsScratch = new Float64Array(state.capacity);

    for (const key of state.buffers.keys()) {
        state.buffers.set(key, new RollingBuffer(state.capacity));
    }
}

function handleFrameMessage(rawMessage) {
    let msg;
    try {
        msg = JSON.parse(rawMessage);
    } catch (e) {
        console.error("Bad WS message:", e);
        return;
    }

    if (msg.type !== "frame") return;

    ensureBuffersSized(msg.fs);

    const frameStartMs = parseFrameTimestamp(msg.date, msg.time);
    if (frameStartMs !== null && msg.fs && msg.n) {
        const frameDurationMs = (msg.n / msg.fs) * 1000;
        state.latestTimestampMs = frameStartMs + frameDurationMs;
        state.latestTimestampSec = state.latestTimestampMs / 1000;
    }

    for (const key of Object.keys(msg.data)) {
        const channel = msg.data[key];
        if (!channel.payload) continue;

        if (!state.buffers.has(key)) {
            state.buffers.set(key, new RollingBuffer(state.capacity));
            state.channelOrder.push(key);
            addChannelToggle(key, channel.name);
        }

        const samples = decodeBase64ToInt16BE(channel.payload);
        state.buffers.get(key).push(samples);
    }

    state.framesThisSecond += 1;
}

// Current live window's absolute time bounds (seconds), anchored to the
// most recent data timestamp rather than a value frozen at chart-build time.
function liveWindowBounds() {
    if (state.latestTimestampSec === null || !state.xAxis) return null;
    return {
        min: state.latestTimestampSec + state.xAxis[0],
        max: state.latestTimestampSec + state.xAxis[state.xAxis.length - 1],
    };
}

function buildChart() {
    const container = document.getElementById("waveform-chart");
    if (!container) return;

    const opts = {
        width: container.clientWidth,
        height: container.clientHeight || 340,
        scales: { 
            x: { time: true } },
            y: { rangle: [-2, 2] }, //locking the y axis so it doesn't keep changing
        axes: [
            { label: "Time", stroke: "#222222", grid: { stroke: "#E7C8B6" }, space: 80 },
            { label: "Volts", stroke: "#222222", grid: { stroke: "#E7C8B6" }, size: 50 },
        ],
        series: [{}],
        cursor: {
            drag: { x: true, y: false, uni: 30 },
        },
        legend: { show: true },
    };

    state.uplot = new uPlot(opts, [new Float64Array(0)], container);

    // Double-click resets zoom back to the live window (anchored to the
    // data's current timestamp, not a frozen absolute range)
    container.addEventListener("dblclick", () => {
        if (!state.uplot) return;
        const bounds = liveWindowBounds();
        if (bounds) state.uplot.setScale("x", bounds);
    });

    window.addEventListener("resize", () => {
        if (!state.uplot) return;
        state.uplot.setSize({ width: container.clientWidth, height: container.clientHeight || 340 });
    });
}

function addChannelToggle(key, name) {
    const list = document.getElementById("channel-toggles");
    if (!list) return;

    const idx = state.channelOrder.indexOf(key);
    const color = CHANNEL_COLORS[idx % CHANNEL_COLORS.length];

    const row = document.createElement("label");
    row.className = "channel-toggle";
    row.innerHTML = `
        <input type="checkbox" checked data-channel="${key}">
        <span class="channel-swatch" style="background:${color}"></span>
        ${name || key}
    `;
    row.querySelector("input").addEventListener("change", (e) => {
        if (e.target.checked) {
            state.hiddenChannels.delete(key);
        } else {
            state.hiddenChannels.add(key);
        }
        rebuildSeries();
    });
    list.appendChild(row);

    rebuildSeries();
}

function rebuildSeries() {
    if (!state.uplot) return;

    const series = [{}];
    for (const key of state.channelOrder) {
        if (state.hiddenChannels.has(key)) continue;
        const idx = state.channelOrder.indexOf(key);
        series.push({
            label: key,
            stroke: CHANNEL_COLORS[idx % CHANNEL_COLORS.length],
            width: 1.5,
            points: { show: false },
        });
    }

    const container = document.getElementById("waveform-chart");
    const prevScale = state.uplot.scales.x;
    state.uplot.destroy();

    const opts = {
        width: container.clientWidth,
        height: container.clientHeight || 340,
        scales: { 
            x: { time: true } },
            y: { range: [-2, 2] },
        axes: [
            { label: "Time", stroke: "#222222", grid: { stroke: "#E7C8B6" }, space: 80 },
            { label: "Volts", stroke: "#222222", grid: { stroke: "#E7C8B6" }, size: 50 },
        ],
        series,
        cursor: { drag: { x: true, y: false, uni: 30 } },
        legend: { show: true },
    };

    const emptyData = [new Float64Array(0)];
    for (let i = 1; i < series.length; i++) emptyData.push(new Float64Array(0));

    state.uplot = new uPlot(opts, emptyData, container);
    if (prevScale && prevScale.min != null) {
        state.uplot.setScale("x", { min: prevScale.min, max: prevScale.max });
    }
}

// function refreshPlot() {
//     if (!state.uplot) return;
//     const bounds = liveWindowBounds();
//     if (bounds) state.uplot.setScale("x", bounds);
// }

function renderLoop(timestamp) {
    requestAnimationFrame(renderLoop);
    state.rafCallCount += 1;
    state.redrawCount += 1; // every animation frame now triggers a redraw attempt

    // my attempt at making the display constant
    // refreshPlot()

    updateDataTimeDisplay();

    if (state.paused || !state.uplot || state.xAxis === null) return;
    if (state.channelOrder.length === 0) return;
    if (state.latestTimestampSec === null) return; // no data yet - nothing to anchor the time axis to

    let maxLen = 0;
    for (const key of state.channelOrder) {
        const buf = state.buffers.get(key);
        if (buf && buf.length > maxLen) maxLen = buf.length;
    }
    if (maxLen === 0) return;

    const offset = state.capacity - maxLen;
    const xAbs = state.xAbsScratch;
    for (let i = 0; i < maxLen; i++) {
        xAbs[i] = state.latestTimestampSec + state.xAxis[offset + i];
    }
    const xView = xAbs.subarray(0, maxLen);
    const data = [xView];

    for (const key of state.channelOrder) {
        if (state.hiddenChannels.has(key)) continue;
        const buf = state.buffers.get(key);
        const view = buf ? buf.view() : new Float32Array(0);
        if (view.length < maxLen) {
            const padded = new Float32Array(maxLen).fill(NaN);
            padded.set(view, maxLen - view.length);
            data.push(padded);
        } else {
            data.push(view);
        }
    }

    // state.uplot.setData(data, false);

    // Batching groups the data update and scale shift into a single, smooth draw
    state.uplot.batch(() => {
        state.uplot.setData(data, false);
        const bounds = liveWindowBounds();
        if (bounds) state.uplot.setScale("x", bounds);
    });
}

// Updates the live data time readout above the chart, in full ISO 8601.
function updateDataTimeDisplay() {
    const el = document.getElementById("data-time-display");
    if (!el) return;
    el.textContent = state.latestTimestampMs !== null
        ? formatISO8601(state.latestTimestampMs)
        : "Waiting for data...";
}

function updateStatsCard() {
    const el = document.getElementById("stream-stats");
    if (!el) return;

    el.innerHTML = `
        <div><strong>Sample rate (Fs):</strong> ${state.fs ?? "-"} Hz</div>
        <div><strong>Data rate (WS):</strong> ${state.currentFrameRate} frames/sec</div>
        <div><strong>Animation loop:</strong> ${state.currentRafRate} calls/sec (browser's true rate)</div>
        <div><strong>Chart redraws:</strong> ${state.currentRedrawRate} /sec (unthrottled - should match animation loop)</div>
        <div><strong>Window:</strong> ${WINDOW_SECONDS}s (${state.capacity ?? "-"} samples)</div>
        <div><strong>Channels:</strong> ${state.channelOrder.length}</div>
    `;
}

function startStatsTicker() {
    setInterval(() => {
        state.currentFrameRate = state.framesThisSecond;
        state.currentRafRate = state.rafCallCount;
        state.currentRedrawRate = state.redrawCount;

        state.framesThisSecond = 0;
        state.rafCallCount = 0;
        state.redrawCount = 0;

        updateStatsCard();
    }, 1000);
}

function renderPipelineStatus(processes) {
    const statusEl = document.getElementById("pipeline-status");
    if (!statusEl || !processes) return;

    const parts = Object.entries(processes).map(
        ([name, running]) => `${name}: ${running ? "running" : "stopped"}`
    );
    statusEl.textContent = parts.join(" | ");
}

async function pollPipelineStatus() {
    try {
        const res = await fetch("/api/status");
        const data = await res.json();
        renderPipelineStatus(data.external_processes);
    } catch (e) {
        // ignore, next poll retries
    }
}

function setupControls() {
    const startBtn = document.getElementById("start-pipeline-btn");
    const statusEl = document.getElementById("pipeline-status");
    if (startBtn) {
        startBtn.addEventListener("click", async () => {
            startBtn.disabled = true;
            startBtn.textContent = "Starting...";
            try {
                const res = await fetch("/api/start-pipeline", { method: "POST" });
                const data = await res.json();
                renderPipelineStatus(data.processes);
            } catch (e) {
                console.error("Failed to start pipeline:", e);
                if (statusEl) statusEl.textContent = "Failed to reach server - see console.";
            } finally {
                startBtn.disabled = false;
                startBtn.textContent = "Start Pipeline";
            }
        });
    }

    pollPipelineStatus();
    setInterval(pollPipelineStatus, 5000);

    const resetBtn = document.getElementById("reset-zoom-btn");
    if (resetBtn) {
        resetBtn.addEventListener("click", () => {
            if (!state.uplot) return;
            const bounds = liveWindowBounds();
            if (bounds) state.uplot.setScale("x", bounds);
        });
    }

    const pauseBtn = document.getElementById("pause-btn");
    if (pauseBtn) {
        pauseBtn.addEventListener("click", () => {
            state.paused = !state.paused;
            pauseBtn.textContent = state.paused ? "Resume" : "Pause";
        });
    }
}

async function stopServer() {
    if (confirm("Are you sure you want to stop the server?")) {
        await fetch('/api/shutdown', { method: 'POST' });
    }
}
window.stopServer = stopServer;

document.addEventListener("DOMContentLoaded", () => {
    updateConnectionStatus();
    connectWebSocket();
    setupControls();
    startStatsTicker();

    try {
        if (typeof uPlot === "undefined") {
            throw new Error("uPlot library did not load (check network/CDN access)");
        }
        buildChart();
    } catch (err) {
        console.error("[chart] failed to initialise:", err);
        const container = document.getElementById("waveform-chart");
        if (container) {
            container.innerHTML =
                '<p class="setup-note" style="padding:1rem;">Chart failed to load: ' +
                err.message +
                '. Data is likely still streaming - check the browser console for details.</p>';
        }
    }

    requestAnimationFrame(renderLoop);
});
