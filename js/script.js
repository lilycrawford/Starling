// script.js

// for the 'stop server' button in nav
async function stopServer() {
    if (confirm("Are you sure you want to stop the server?")) {
        await fetch('/api/shutdown', { method: 'POST' });
        document.body.innerHTML = "<h1>Server Has Been Shutdown</h1>";
    }
}

window.stopServer = stopServer;
