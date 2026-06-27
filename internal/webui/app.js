// app.js — lógica de la web UI del plano de control del Edge (Plan 007, T5).
//
// Todo es JS vanilla (fetch + EventSource), sin dependencias ni CDN. La página vive en el MISMO origen
// loopback que el supervisor (http://127.0.0.1:8765), así que las rutas /v1/* se piden relativas y el
// supervisor decide qué atiende él (/v1/daemon/*) y qué proxya al núcleo (/v1/health, /v1/sessions, …).
//
// Contrato real de los endpoints (verificado en el código Go):
//   GET  /v1/daemon/status -> {state:"running"|"stopped", pid?:number, healthy:boolean}  (cmd/wapp-ctl/respond.go)
//   POST /v1/daemon/start  -> mismo cuerpo daemonStatusResponse
//   POST /v1/daemon/stop   -> mismo cuerpo daemonStatusResponse
//   GET  /v1/health        -> 200 {status:"ok",version} | 503 {"error":{"code":"daemon_down",...}}
//   GET  /v1/sessions      -> {sessions:[{jid,state,paired_at?,updated_at?}]}             (server/handlers.go)
//   POST /v1/sessions/pair -> {id,status,qr}  (qr = data-URL PNG o "")                    (server/pair.go)
//   GET  /v1/sessions/{id}/pair -> {status:"pending"|"success"|"error", qr, error?}       (server/pair.go)
//   GET  /v1/logs          -> SSE; cada evento `data: <línea de log>`                      (logsink/handler.go)

"use strict";

// ----------------------------- Helpers de DOM -----------------------------
const $ = (id) => document.getElementById(id);

const el = {
  daemonBadge: $("daemon-badge"),
  daemonDetail: $("daemon-detail"),
  btnStart: $("btn-start"),
  btnStop: $("btn-stop"),
  sessionsBody: $("sessions-body"),
  btnRefreshSessions: $("btn-refresh-sessions"),
  btnPair: $("btn-pair"),
  pairStatus: $("pair-status"),
  pairQrWrap: $("pair-qr-wrap"),
  pairQr: $("pair-qr"),
  logsState: $("logs-state"),
  logs: $("logs"),
};

// Estado de la UI (no del daemon): nos sirve para no relanzar sondeos/streams en cada tick.
const ui = {
  daemonRunning: false, // último estado conocido del núcleo
  pollingPair: false, // hay un emparejamiento en curso
  pairTimer: null, // setInterval del poll de pairing
  lastQr: "", // último data-URL pintado (para no recargar la <img> si no rotó)
  logsSource: null, // EventSource activo de /v1/logs
};

// setBadge aplica una clase de color y un texto a un <span class="badge">.
function setBadge(node, kind, text) {
  node.className = "badge badge-" + kind;
  node.textContent = text;
}

// ----------------------------- Estado del daemon -----------------------------

// fetchStatus consulta el supervisor (siempre vivo aunque el núcleo esté caído) y pinta el estado.
// Devuelve true si el núcleo está corriendo.
async function fetchStatus() {
  try {
    const res = await fetch("/v1/daemon/status", { cache: "no-store" });
    if (!res.ok) throw new Error("status HTTP " + res.status);
    const s = await res.json(); // {state, pid?, healthy}
    applyDaemonState(s);
    return s.state === "running";
  } catch (err) {
    // Si ni el supervisor responde, es un fallo gordo (la página se sirve desde él): lo mostramos.
    setBadge(el.daemonBadge, "down", "Sin supervisor");
    el.daemonDetail.textContent = "No se pudo contactar al supervisor: " + err.message;
    setDaemonControls(false, false);
    return false;
  }
}

// applyDaemonState refleja {state,pid,healthy} en el badge, el detalle y los controles, y dispara
// las acciones derivadas (sesiones, logs) cuando el núcleo pasa a corriendo.
function applyDaemonState(s) {
  const running = s.state === "running";
  const wasRunning = ui.daemonRunning;
  ui.daemonRunning = running;

  if (running) {
    if (s.healthy) {
      setBadge(el.daemonBadge, "ok", "Corriendo");
      el.daemonDetail.textContent = "El núcleo responde (PID " + (s.pid || "?") + ").";
    } else {
      // Corriendo pero /v1/health aún no devuelve 200: arrancando / no-ready.
      setBadge(el.daemonBadge, "warn", "Arrancando…");
      el.daemonDetail.textContent = "El núcleo arrancó (PID " + (s.pid || "?") + ") pero aún no está listo.";
    }
    setDaemonControls(false, true); // start off, stop on
  } else {
    setBadge(el.daemonBadge, "down", "Detenido");
    el.daemonDetail.textContent = "El núcleo no está corriendo. Pulsa «Arrancar».";
    setDaemonControls(true, false); // start on, stop off
  }

  // Transición detenido -> corriendo: (re)conectar logs y refrescar sesiones.
  if (running && !wasRunning) {
    connectLogs();
    refreshSessions();
  }
  // Transición corriendo -> detenido: degradar las secciones dependientes del núcleo.
  if (!running && wasRunning) {
    degradeCoreSections();
  }
  // Estado inicial detenido: dejar las secciones en su forma degradada.
  if (!running) {
    el.btnPair.disabled = true;
  } else {
    el.btnPair.disabled = ui.pollingPair; // habilitado salvo que ya haya un pairing en curso
  }
}

// setDaemonControls habilita/inhabilita los botones Arrancar/Detener de forma exclusiva.
function setDaemonControls(canStart, canStop) {
  el.btnStart.disabled = !canStart;
  el.btnStop.disabled = !canStop;
}

// degradeCoreSections deja sesiones/logs/pair en estado seguro cuando el núcleo cae (no rompe la UI).
function degradeCoreSections() {
  el.sessionsBody.innerHTML = '<tr><td colspan="4" class="muted">Daemon detenido.</td></tr>';
  setBadge(el.logsState, "down", "Desconectado");
  disconnectLogs();
  stopPairPolling();
  el.pairStatus.textContent = "Arranca el daemon antes de emparejar.";
  el.pairStatus.className = "detail muted";
  el.pairQrWrap.classList.add("hidden");
}

// ----------------------------- Arrancar / Detener -----------------------------

async function startDaemon() {
  setDaemonControls(false, false);
  setBadge(el.daemonBadge, "warn", "Arrancando…");
  el.daemonDetail.textContent = "Lanzando el núcleo, espera unos segundos…";
  try {
    const res = await fetch("/v1/daemon/start", { method: "POST" });
    const s = await res.json();
    if (!res.ok) {
      // El supervisor responde {"error":{code,message}} (p. ej. start_failed).
      const code = s && s.error ? s.error.code : "error";
      throw new Error(code);
    }
    applyDaemonState(s);
  } catch (err) {
    el.daemonDetail.textContent = "No se pudo arrancar el núcleo: " + err.message;
  }
  // El arranque puede tardar en quedar healthy: re-sondeamos un par de veces.
  scheduleStartupRepolls();
}

async function stopDaemon() {
  setDaemonControls(false, false);
  setBadge(el.daemonBadge, "warn", "Deteniendo…");
  try {
    const res = await fetch("/v1/daemon/stop", { method: "POST" });
    const s = await res.json();
    if (!res.ok) {
      const code = s && s.error ? s.error.code : "error";
      throw new Error(code);
    }
    applyDaemonState(s);
  } catch (err) {
    el.daemonDetail.textContent = "No se pudo detener el núcleo: " + err.message;
    fetchStatus();
  }
}

// scheduleStartupRepolls re-consulta el estado tras el arranque (el núcleo tarda en quedar healthy).
function scheduleStartupRepolls() {
  let n = 0;
  const t = setInterval(() => {
    n += 1;
    fetchStatus();
    if (n >= 5) clearInterval(t); // ~5 reintentos espaciados por el intervalo del setInterval
  }, 1500);
}

// ----------------------------- Sesiones -----------------------------

async function refreshSessions() {
  if (!ui.daemonRunning) {
    el.sessionsBody.innerHTML = '<tr><td colspan="4" class="muted">Daemon detenido.</td></tr>';
    return;
  }
  try {
    const res = await fetch("/v1/sessions", { cache: "no-store" });
    if (res.status === 503) {
      // El proxy traduce núcleo-caído a 503 daemon_down: degradamos sin romper.
      el.sessionsBody.innerHTML = '<tr><td colspan="4" class="muted">Daemon no disponible.</td></tr>';
      return;
    }
    if (!res.ok) throw new Error("HTTP " + res.status);
    const data = await res.json(); // {sessions:[{jid,state,paired_at?,updated_at?}]}
    renderSessions(data.sessions || []);
  } catch (err) {
    el.sessionsBody.innerHTML =
      '<tr><td colspan="4" class="muted">Error al listar: ' + escapeHtml(err.message) + "</td></tr>";
  }
}

function renderSessions(sessions) {
  if (sessions.length === 0) {
    el.sessionsBody.innerHTML = '<tr><td colspan="4" class="muted">Sin sesiones.</td></tr>';
    return;
  }
  el.sessionsBody.innerHTML = sessions
    .map(
      (s) =>
        "<tr><td>" +
        escapeHtml(s.jid || "—") +
        "</td><td>" +
        escapeHtml(s.state || "—") +
        "</td><td>" +
        escapeHtml(s.paired_at || "—") +
        "</td><td>" +
        escapeHtml(s.updated_at || "—") +
        "</td></tr>"
    )
    .join("");
}

// ----------------------------- Emparejar (QR + poll) -----------------------------

async function startPairing() {
  if (!ui.daemonRunning) {
    el.pairStatus.textContent = "Arranca el daemon antes de emparejar.";
    el.pairStatus.className = "detail muted";
    return;
  }
  el.btnPair.disabled = true;
  el.pairStatus.textContent = "Generando código QR…";
  el.pairStatus.className = "detail";
  el.pairQrWrap.classList.add("hidden");
  ui.lastQr = "";

  try {
    const res = await fetch("/v1/sessions/pair", { method: "POST" });
    if (res.status === 409) {
      el.pairStatus.textContent = "Ya hay un emparejamiento en curso.";
      el.btnPair.disabled = false;
      return;
    }
    if (res.status === 503) {
      el.pairStatus.textContent = "Daemon no disponible. Arráncalo primero.";
      el.pairStatus.className = "detail muted";
      el.btnPair.disabled = false;
      return;
    }
    if (!res.ok) throw new Error("HTTP " + res.status);
    const data = await res.json(); // {id,status,qr}
    showQr(data.qr);
    el.pairStatus.textContent = "Escanea el QR con WhatsApp (Dispositivos vinculados).";
    pollPairing(data.id);
  } catch (err) {
    el.pairStatus.textContent = "No se pudo iniciar el emparejamiento: " + err.message;
    el.pairStatus.className = "detail";
    el.btnPair.disabled = false;
  }
}

// pollPairing consulta GET /v1/sessions/{id}/pair cada 2s: refresca el QR si rota y resuelve en
// success/error. Un id desconocido (404) o un núcleo caído (503) terminan el poll con un mensaje.
function pollPairing(id) {
  ui.pollingPair = true;
  stopPairPolling(); // por si hubiera uno previo
  ui.pairTimer = setInterval(async () => {
    try {
      const res = await fetch("/v1/sessions/" + encodeURIComponent(id) + "/pair", { cache: "no-store" });
      if (res.status === 404) {
        finishPairing("El emparejamiento expiró o no existe.", false);
        return;
      }
      if (res.status === 503) {
        finishPairing("Daemon caído durante el emparejamiento.", false);
        return;
      }
      if (!res.ok) throw new Error("HTTP " + res.status);
      const data = await res.json(); // {status, qr, error?}
      if (data.status === "pending") {
        if (data.qr) showQr(data.qr); // refresca solo si el QR cambió (ver showQr)
      } else if (data.status === "success") {
        finishPairing("Emparejado correctamente.", true);
        refreshSessions();
      } else if (data.status === "error") {
        finishPairing("Error de emparejamiento: " + (data.error || "desconocido"), false);
      }
    } catch (err) {
      finishPairing("Error consultando el emparejamiento: " + err.message, false);
    }
  }, 2000);
}

// showQr pinta el data-URL en la <img> solo si cambió, para no parpadear en cada poll.
function showQr(dataUrl) {
  if (!dataUrl || dataUrl === ui.lastQr) return;
  ui.lastQr = dataUrl;
  el.pairQr.src = dataUrl;
  el.pairQrWrap.classList.remove("hidden");
}

// finishPairing cierra el ciclo de emparejamiento con un mensaje (ok=true => éxito).
function finishPairing(msg, ok) {
  stopPairPolling();
  ui.pollingPair = false;
  ui.lastQr = "";
  el.pairQrWrap.classList.add("hidden");
  el.pairStatus.textContent = (ok ? "✅ " : "⚠️ ") + msg;
  el.pairStatus.className = "detail";
  el.btnPair.disabled = !ui.daemonRunning;
}

function stopPairPolling() {
  if (ui.pairTimer) {
    clearInterval(ui.pairTimer);
    ui.pairTimer = null;
  }
}

// ----------------------------- Logs en vivo (SSE) -----------------------------

// connectLogs abre (o reabre) el EventSource de /v1/logs. Cada evento `data:` es una línea de log.
function connectLogs() {
  if (ui.logsSource) return; // ya conectado
  setBadge(el.logsState, "warn", "Conectando…");
  const src = new EventSource("/v1/logs");
  ui.logsSource = src;

  src.onopen = () => setBadge(el.logsState, "ok", "Conectado");

  src.onmessage = (ev) => {
    appendLog(ev.data);
  };

  src.onerror = () => {
    // EventSource reintenta solo, pero si el núcleo cayó cerramos para no spamear y reconectamos
    // cuando el daemon vuelva (lo detecta el sondeo de estado y llama a connectLogs de nuevo).
    setBadge(el.logsState, "down", "Desconectado");
    if (!ui.daemonRunning) disconnectLogs();
  };
}

function disconnectLogs() {
  if (ui.logsSource) {
    ui.logsSource.close();
    ui.logsSource = null;
  }
}

// appendLog añade una línea al <pre> y mantiene el auto-scroll al fondo. Acota el buffer del DOM.
function appendLog(line) {
  const atBottom = el.logs.scrollTop + el.logs.clientHeight >= el.logs.scrollHeight - 8;
  el.logs.textContent += line + "\n";
  // Recorta para no crecer sin límite (deja las últimas ~600 líneas).
  const lines = el.logs.textContent.split("\n");
  if (lines.length > 600) {
    el.logs.textContent = lines.slice(lines.length - 600).join("\n");
  }
  if (atBottom) el.logs.scrollTop = el.logs.scrollHeight;
}

// ----------------------------- Utilidades -----------------------------

// escapeHtml evita inyectar markup desde datos del backend (JIDs, mensajes de error).
function escapeHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// ----------------------------- Arranque de la UI -----------------------------

el.btnStart.addEventListener("click", startDaemon);
el.btnStop.addEventListener("click", stopDaemon);
el.btnPair.addEventListener("click", startPairing);
el.btnRefreshSessions.addEventListener("click", refreshSessions);

// Primer sondeo inmediato + bucle de estado cada 3s (el supervisor siempre responde aunque el núcleo
// esté caído, así que este loop nunca deja la UI en blanco).
fetchStatus();
setInterval(fetchStatus, 3000);
