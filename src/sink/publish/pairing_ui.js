"use strict";
const el = id => document.getElementById(id);
let handoff = null;
try {
  const hash = new URLSearchParams(location.hash.slice(1));
  const code = hash.get("code");
  history.replaceState(null, "", location.pathname);
  if (code) {
    if (!/^pair_[a-f0-9]{32}\.[a-f0-9]{64}$/.test(code)) throw new Error();
    handoff = {code};
    el("intro").textContent = "Connect this controller to your selected robot. Then return to the fleet page to approve it.";
    el("connect").hidden = false;
  }
} catch { el("error").textContent = "This setup link is invalid. Create a new connection from your robot’s fleet page."; }
const messages = {
  ready: "Ready to connect.", connecting: "Contacting your fleet service…",
  retrying: "Connection interrupted. Bagel is retrying automatically…",
  awaiting_confirmation: "Waiting for your approval on the fleet page.",
  connected: "Connected. Your certificate is installed.",
  enrolled: "This controller is already connected. Return to your fleet page to view its status.",
  paused: "Connection paused. Retry to continue with the same identity.",
  error: "Connection could not finish. Check the fleet page for an expired or cancelled request, or retry after restoring connectivity."
};
function show(s) {
  if (s.fleet_service) el("destination").textContent = "Fleet service: " + s.fleet_service;
  el("status").textContent = messages[s.status] || "Checking connection…";
  el("identity").hidden = !s.public_key_sha256 || ["connected", "enrolled"].includes(s.status);
  el("fingerprint").textContent = s.public_key_sha256 || "";
  el("controller").textContent = s.controller_id ? "Controller: " + s.controller_id : "";
  el("resume").hidden = !["paused", "error"].includes(s.status);
  if (!["ready", "paused", "error"].includes(s.status)) el("connect").hidden = true;
  if (s.status === "connected") {
    handoff = null;
    el("intro").textContent = s.streams_reloaded ? "Existing data streams have been reloaded. Check your fleet page for live data." : "Your identity is ready. Data streaming still needs attention in the controller configuration.";
  }
}
async function start(data) {
  el("error").textContent = "";
  el("connect").disabled = el("resume").disabled = true;
  try {
    const r = await fetch("/fleet/connect/start", {method: "POST", headers: {"Content-Type": "application/json", "X-Bagel-CSRF": document.querySelector('meta[name="bagel-csrf"]').content}, body: JSON.stringify(data)});
    const s = await r.json();
    if (!r.ok) throw new Error(s.error || "Connection could not start.");
    show(s);
  } catch(e) { el("error").textContent = e.message; }
  finally { el("connect").disabled = el("resume").disabled = false; }
}
el("connect").onclick = () => handoff && start(handoff);
el("resume").onclick = () => start({resume: true});
async function poll() {
  try { const r = await fetch("/fleet/connect/status"); if (!r.ok) throw new Error(); show(await r.json()); }
  catch { el("status").textContent = "Cannot reach Bagel. Check that the controller is running; this page will retry."; }
  setTimeout(poll, 2500);
}
poll();
