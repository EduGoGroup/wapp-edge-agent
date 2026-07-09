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
//   GET    /v1/sessions      -> {sessions:[{jid,state,paired_at?,updated_at?}]}           (server/handlers.go)
//   POST   /v1/sessions/pair -> {id,status,qr}  (qr = data-URL PNG o "")                  (server/pair.go)
//   GET    /v1/sessions/{id}/pair -> {status:"pending"|"success"|"error", qr, error?}     (server/pair.go)
//   DELETE /v1/sessions/{id} -> 200 {jid,unlinked,previous_state?,remote_logout} | 404    (server/unlink.go)
//   GET    /v1/logs          -> SSE; cada evento `data: <línea de log>`                    (logsink/handler.go)
//
// NOTA (honestidad): /v1/logs es el log GLOBAL del daemon, no filtrado por sesión (ver index.html).

"use strict";

// ----------------------------- Helpers de DOM -----------------------------
const $ = (id) => document.getElementById(id);

const el = {
  daemonBadge: $("daemon-badge"),
  daemonDetail: $("daemon-detail"),
  btnStart: $("btn-start"),
  btnStop: $("btn-stop"),
  sessionsBody: $("sessions-body"),
  sessionsNotice: $("sessions-notice"),
  btnRefreshSessions: $("btn-refresh-sessions"),
  logsScope: $("logs-scope"),
  btnPair: $("btn-pair"),
  pairStatus: $("pair-status"),
  pairQrWrap: $("pair-qr-wrap"),
  pairQr: $("pair-qr"),
  logsState: $("logs-state"),
  logs: $("logs"),
  // Onboarding / enrolar (Plan 023 · T1) + secciones del dashboard (para conmutar la vista).
  enrollSection: $("section-enroll"),
  enrollForm: $("enroll-form"),
  enrollCode: $("enroll-code"),
  enrollSubmit: $("enroll-submit"),
  enrollStatus: $("enroll-status"),
  sectionSessions: $("section-sessions"),
  sectionPair: $("section-pair"),
  sectionLogs: $("section-logs"),
};

// Estado de la UI (no del daemon): nos sirve para no relanzar sondeos/streams en cada tick.
const ui = {
  daemonRunning: false, // último estado conocido del núcleo
  daemonHealthy: false, // último 'healthy' conocido (para consultar el enroll status en la transición)
  enrolled: true, // ¿hay credencial mTLS? default true = dashboard; el núcleo lo confirma en /v1/enroll/status
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

  // Onboarding (T1): cuando el núcleo pasa a healthy, consulta si ya hay credencial mTLS para decidir
  // entre la pantalla "enrolar" y el dashboard. Solo se consulta en la TRANSICIÓN a healthy (no en cada
  // tick); applyView refleja la vista vigente en cada llamada.
  const healthy = running && !!s.healthy;
  if (healthy && !ui.daemonHealthy) {
    fetchEnrollStatus();
  }
  ui.daemonHealthy = healthy;
  applyView();
}

// setDaemonControls habilita/inhabilita los botones Arrancar/Detener de forma exclusiva.
function setDaemonControls(canStart, canStop) {
  el.btnStart.disabled = !canStart;
  el.btnStop.disabled = !canStop;
}

// degradeCoreSections deja sesiones/logs/pair en estado seguro cuando el núcleo cae (no rompe la UI).
function degradeCoreSections() {
  el.sessionsBody.innerHTML = '<tr><td colspan="5" class="muted">Daemon detenido.</td></tr>';
  setBadge(el.logsState, "down", "Desconectado");
  disconnectLogs();
  stopPairPolling();
  el.pairStatus.textContent = "Arranca el daemon antes de emparejar.";
  el.pairStatus.className = "detail muted";
  el.pairQrWrap.classList.add("hidden");
}

// ----------------------------- Onboarding / enrolar (Plan 023 · T1) -----------------------------

// applyView alterna entre la pantalla "enrolar" (sin credencial) y el dashboard. Solo oculta el
// dashboard cuando el núcleo está vivo Y sabemos que NO hay credencial; en cualquier otro caso (daemon
// caído, credencial presente o estado aún desconocido) deja el dashboard como hasta ahora (con su propio
// degradado si el núcleo cae), para no regresar a los usuarios ya enrolados a una pantalla en blanco.
function applyView() {
  const onboarding = ui.daemonRunning && ui.enrolled === false;
  toggleSection(el.enrollSection, onboarding);
  toggleSection(el.sectionSessions, !onboarding);
  toggleSection(el.sectionPair, !onboarding);
  toggleSection(el.sectionLogs, !onboarding);
}

// toggleSection muestra u oculta una <section> con la clase .hidden (no-op si el nodo no existe).
function toggleSection(node, show) {
  if (node) node.classList.toggle("hidden", !show);
}

// fetchEnrollStatus pregunta al núcleo si ya hay credencial mTLS (GET /v1/enroll/status). Un 503 (daemon
// caído a mitad) deja el estado como estaba (se reintenta en la próxima transición a healthy); cualquier
// otro fallo es fail-open al dashboard, para no atrapar al usuario en una pantalla en blanco.
async function fetchEnrollStatus() {
  try {
    const res = await fetch("/v1/enroll/status", { cache: "no-store" });
    if (res.status === 503) return; // daemon down
    if (!res.ok) throw new Error("HTTP " + res.status);
    const data = await res.json(); // {enrolled: bool}
    ui.enrolled = !!data.enrolled;
  } catch (_err) {
    ui.enrolled = true; // fail-open: ante la duda, muestra el dashboard
  }
  applyView();
}

// submitEnroll envía el activation code a POST /v1/enroll (reusa el enroll REAL del núcleo). Al éxito
// marca enrolled y conmuta al dashboard; los errores se explican en la propia pantalla sin recargar.
async function submitEnroll(ev) {
  ev.preventDefault();
  const code = el.enrollCode.value.trim();
  if (!code) {
    setEnrollStatus("Introduce el activation code que te dio el panel.", true);
    return;
  }
  el.enrollSubmit.disabled = true;
  setEnrollStatus("Enrolando este equipo contra la nube…", false);
  try {
    const res = await fetch("/v1/enroll", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ activation_code: code }),
    });
    const data = await res.json().catch(() => ({}));
    if (res.status === 503) {
      setEnrollStatus("El servicio no está disponible. Arranca el daemon e inténtalo de nuevo.", true);
      el.enrollSubmit.disabled = false;
      return;
    }
    if (!res.ok) {
      const msg = data && data.error ? data.error.message || data.error.code : "HTTP " + res.status;
      throw new Error(msg);
    }
    ui.enrolled = true;
    el.enrollCode.value = "";
    setEnrollStatus("✅ Equipo enrolado. Ya puedes emparejar un teléfono.", false);
    applyView();
  } catch (err) {
    setEnrollStatus("⚠️ No se pudo enrolar: " + err.message, true);
    el.enrollSubmit.disabled = false;
  }
}

// setEnrollStatus escribe el mensaje de estado de la pantalla enrolar (isError pinta en tono de error).
function setEnrollStatus(msg, isError) {
  if (!el.enrollStatus) return;
  el.enrollStatus.textContent = msg;
  el.enrollStatus.className = "detail" + (isError ? " notice-error" : " muted");
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
    el.sessionsBody.innerHTML = '<tr><td colspan="5" class="muted">Daemon detenido.</td></tr>';
    return;
  }
  try {
    const res = await fetch("/v1/sessions", { cache: "no-store" });
    if (res.status === 503) {
      // El proxy traduce núcleo-caído a 503 daemon_down: degradamos sin romper.
      el.sessionsBody.innerHTML = '<tr><td colspan="5" class="muted">Daemon no disponible.</td></tr>';
      return;
    }
    if (!res.ok) throw new Error("HTTP " + res.status);
    const data = await res.json(); // {sessions:[{jid,state,paired_at?,updated_at?}]}
    renderSessions(data.sessions || []);
  } catch (err) {
    el.sessionsBody.innerHTML =
      '<tr><td colspan="5" class="muted">Error al listar: ' + escapeHtml(err.message) + "</td></tr>";
  }
}

function renderSessions(sessions) {
  updateLogsScope(sessions);
  if (sessions.length === 0) {
    el.sessionsBody.innerHTML = '<tr><td colspan="5" class="muted">Sin sesiones.</td></tr>';
    return;
  }
  // El botón lleva data-jid/data-action; el click se atiende por delegación (onSessionsClick), así
  // los listeners sobreviven a los re-render por innerHTML.
  el.sessionsBody.innerHTML = sessions
    .map((s) => {
      const jid = s.jid || "";
      const jidAttr = escapeHtml(jid).replace(/'/g, "&#39;");
      return (
        "<tr><td>" +
        escapeHtml(jid || "—") +
        "</td><td>" +
        escapeHtml(s.state || "—") +
        "</td><td>" +
        escapeHtml(s.paired_at || "—") +
        "</td><td>" +
        escapeHtml(s.updated_at || "—") +
        '</td><td class="session-actions">' +
        (jid
          ? '<button type="button" class="secondary small danger" data-action="unlink" data-jid="' +
            jidAttr +
            '">Eliminar y limpiar</button>'
          : "—") +
        "</td></tr>"
      );
    })
    .join("");
}

// updateLogsScope refleja en la etiqueta del visor de logs el JID de la sesión "activa" (la primera
// activa, o la primera a secas). HONESTIDAD: los logs siguen siendo el log GLOBAL del daemon; esto solo
// vincula visualmente el visor a la sesión vigente (no es un filtrado real; ver index.html / MP-01).
function updateLogsScope(sessions) {
  if (!el.logsScope) return;
  const active = sessions.find((s) => s.state === "active") || sessions[0];
  el.logsScope.textContent = active && active.jid ? active.jid : "—";
}

// onSessionsClick atiende por DELEGACIÓN los botones de la tabla de sesiones (sobreviven a los
// re-render). "unlink" abre una confirmación in-page (NO window.confirm: no bloquea la UI).
function onSessionsClick(ev) {
  const btn = ev.target.closest("button[data-action]");
  if (!btn) return;
  const jid = btn.getAttribute("data-jid") || "";
  const action = btn.getAttribute("data-action");
  if (action === "unlink") {
    showUnlinkConfirm(btn, jid);
  } else if (action === "confirm-unlink") {
    deleteSession(jid);
  } else if (action === "cancel-unlink") {
    refreshSessions();
  }
}

// showUnlinkConfirm sustituye la celda de acciones de la fila por una confirmación inline (Confirmar /
// Cancelar), evitando un diálogo bloqueante. Confirmar dispara deleteSession; Cancelar re-renderiza.
function showUnlinkConfirm(btn, jid) {
  const cell = btn.closest("td");
  if (!cell) return;
  const jidAttr = escapeHtml(jid).replace(/'/g, "&#39;");
  cell.innerHTML =
    '<span class="confirm-text">¿Desvincular y borrar estado local?</span> ' +
    '<button type="button" class="small danger" data-action="confirm-unlink" data-jid="' +
    jidAttr +
    '">Confirmar</button> ' +
    '<button type="button" class="small secondary" data-action="cancel-unlink" data-jid="' +
    jidAttr +
    '">Cancelar</button>';
}

// deleteSession ejecuta DELETE /v1/sessions/{jid}: desvincula (logout remoto best-effort) y limpia el
// estado local (device + registro + DEK). Refresca la lista y el estado del daemon al terminar.
async function deleteSession(jid) {
  showSessionsNotice("Desvinculando " + jid + "…", false);
  try {
    const res = await fetch("/v1/sessions/" + encodeURIComponent(jid), { method: "DELETE" });
    if (res.status === 404) {
      showSessionsNotice("La sesión ya no existe (nada que limpiar).", false);
      refreshSessions();
      return;
    }
    if (res.status === 503) {
      showSessionsNotice("Daemon no disponible. Arráncalo e inténtalo de nuevo.", true);
      return;
    }
    if (!res.ok) throw new Error("HTTP " + res.status);
    const data = await res.json(); // {jid, unlinked, previous_state?, remote_logout}
    const logout =
      data.remote_logout === "ok"
        ? " Logout remoto OK (WhatsApp soltó el dispositivo)."
        : data.remote_logout === "failed"
        ? " El logout remoto falló, pero el estado local quedó limpio."
        : " Sin cliente vivo: solo se limpió el estado local.";
    showSessionsNotice("✅ Sesión desvinculada y estado local limpiado." + logout, false);
    refreshSessions();
    fetchStatus();
  } catch (err) {
    showSessionsNotice("⚠️ No se pudo desvincular: " + err.message, true);
  }
}

// showSessionsNotice muestra un aviso bajo la tabla de sesiones (isError pinta en tono de error).
function showSessionsNotice(msg, isError) {
  if (!el.sessionsNotice) return;
  el.sessionsNotice.textContent = msg;
  el.sessionsNotice.className = "detail" + (isError ? " notice-error" : " muted");
  el.sessionsNotice.classList.remove("hidden");
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
el.sessionsBody.addEventListener("click", onSessionsClick);
el.enrollForm.addEventListener("submit", submitEnroll); // onboarding sin terminal (T1)

// Primer sondeo inmediato + bucle de estado cada 3s (el supervisor siempre responde aunque el núcleo
// esté caído, así que este loop nunca deja la UI en blanco).
fetchStatus();
setInterval(fetchStatus, 3000);
