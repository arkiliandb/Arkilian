// Arkilian Control — tenant dashboard. Vanilla JS, no build step, no
// external dependencies. Talks to the same-origin /v1 API. The session
// token lives in localStorage and expires after 24h (server-side).
"use strict";

const $ = (id) => document.getElementById(id);

const state = {
  token: localStorage.getItem("ark_token") || "",
  email: localStorage.getItem("ark_email") || "",
  dbID: null,
  pollTimer: null,
};

const STATUS = {
  active: { label: "Active", cls: "s-active" },
  stale:  { label: "Stale",  cls: "s-stale" },
  quiet:  { label: "Quiet",  cls: "s-quiet" },
  never:  { label: "Never seen", cls: "s-never" },
};

// ── Helpers ─────────────────────────────────────────────────────────

async function api(path, opts = {}) {
  const headers = Object.assign({}, opts.headers || {});
  if (state.token) headers["Authorization"] = "Bearer " + state.token;
  const res = await fetch(path, Object.assign({}, opts, { headers }));
  if (res.status === 401) {
    setToken("");
    showLogin("Session expired — sign in again.");
    throw new Error("unauthorized");
  }
  if (!res.ok) {
    let msg = "HTTP " + res.status;
    try {
      const body = await res.json();
      if (body.error) msg = body.error;
    } catch (_) { /* non-JSON error body */ }
    throw new Error(msg);
  }
  return res.json();
}

function setToken(token, email) {
  state.token = token || "";
  state.email = email || "";
  if (token) {
    localStorage.setItem("ark_token", token);
    localStorage.setItem("ark_email", state.email);
  } else {
    localStorage.removeItem("ark_token");
    localStorage.removeItem("ark_email");
  }
}

function esc(s) {
  return String(s == null ? "" : s)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;")
    .replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function fmtNum(n) {
  n = Number(n) || 0;
  if (n >= 1e9) return (n / 1e9).toFixed(1) + "B";
  if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (n >= 1e3) return (n / 1e3).toFixed(1) + "k";
  return String(n);
}

function fmtBytes(b) {
  b = Number(b) || 0;
  if (!b) return "0 B";
  const u = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  while (b >= 1024 && i < u.length - 1) { b /= 1024; i++; }
  return (b >= 10 || i === 0 ? b.toFixed(0) : b.toFixed(1)) + " " + u[i];
}

function relTime(unix) {
  if (!unix) return "never";
  const d = Math.floor(Date.now() / 1000) - unix;
  if (d < 0) return "just now";
  if (d < 60) return d + "s ago";
  if (d < 3600) return Math.floor(d / 60) + "m ago";
  if (d < 86400) return Math.floor(d / 3600) + "h ago";
  return Math.floor(d / 86400) + "d ago";
}

function badgeHtml(status) {
  const s = STATUS[status] || STATUS.never;
  return '<span class="badge ' + s.cls + '">' + s.label + "</span>";
}

// Inline-SVG bar chart. values: array of numbers (oldest first).
function barChart(values, opts = {}) {
  const w = opts.w || 90;
  const h = opts.h || 26;
  const max = Math.max.apply(null, values.concat([1]));
  const n = values.length;
  if (!n) return '<span class="muted">no data</span>';
  const bw = Math.max(2, Math.floor(w / n) - 2);
  let bars = "";
  for (let i = 0; i < n; i++) {
    const bh = Math.max(1, Math.round((values[i] / max) * (h - 5)));
    const x = i * (bw + 2);
    bars += '<rect x="' + x + '" y="' + (h - bh) + '" width="' + bw +
            '" height="' + bh + '" rx="1"></rect>';
  }
  return '<svg viewBox="0 0 ' + w + " " + h + '" width="' + w + '" height="' + h +
         '" role="img" aria-label="bar chart">' + bars + "</svg>";
}

// daily[] comes back newest-first from the API; chart wants oldest-first.
function seriesFor(daily, days) {
  const map = new Map(daily.map((d) => [d.day, d]));
  const out = [];
  for (let i = days - 1; i >= 0; i--) {
    const d = new Date(Date.now() - i * 86400000);
    const key = d.toISOString().slice(0, 10);
    out.push((map.get(key) || {}).entries || 0);
  }
  return out;
}

function fmtTs(ts, received) {
  const base = ts ? ts / 1e9 : received;
  if (!base) return "–";
  const d = new Date(base * 1000);
  return d.toLocaleString();
}

// ── Views ───────────────────────────────────────────────────────────

function showLogin(msg) {
  state.dbID = null;
  stopPolling();
  $("view-login").classList.remove("hidden");
  $("view-dash").classList.add("hidden");
  if (msg) {
    $("login-error").textContent = msg;
    $("login-error").classList.remove("hidden");
  }
}

function showDash() {
  $("view-login").classList.add("hidden");
  $("view-dash").classList.remove("hidden");
  $("user-email").textContent = state.email;
  goOverview();
  startPolling();
}

function goOverview() {
  state.dbID = null;
  $("view-overview").classList.remove("hidden");
  $("view-detail").classList.add("hidden");
}

function goDetail(dbID) {
  state.dbID = dbID;
  $("view-overview").classList.add("hidden");
  $("view-detail").classList.remove("hidden");
  loadDetail(dbID);
}

// ── Overview ────────────────────────────────────────────────────────

async function loadSummary() {
  const dbs = await api("/v1/monitor/summary");
  renderSummary(dbs);
}

function renderSummary(dbs) {
  const tbody = $("db-table").querySelector("tbody");
  tbody.innerHTML = "";

  let active = 0, today = 0, total = 0;
  for (const d of dbs) {
    active += d.status === "active" ? 1 : 0;
    today += d.entries_today || 0;
    total += d.total_entries || 0;

    const tr = document.createElement("tr");
    const chart = barChart(seriesFor(d.last7 || [], 7));
    tr.innerHTML =
      "<td><span class='db-name'>" + esc(d.name) + "</span>" +
        "<span class='mono muted db-id'>" + esc(d.db_id) + "</span></td>" +
      "<td>" + badgeHtml(d.status) + "</td>" +
      "<td>" + fmtNum(d.entries_today) + "</td>" +
      "<td>" + fmtNum(d.total_entries) + "</td>" +
      "<td class='muted'>" + relTime(d.last_seen) + "</td>" +
      "<td>" + (d.snapshots || 0) + "</td>" +
      "<td class='chart-cell'>" + chart + "</td>";
    tr.addEventListener("click", () => goDetail(d.db_id));
    tbody.appendChild(tr);
  }

  $("stat-dbs").textContent = dbs.length;
  $("stat-active").textContent = active;
  $("stat-today").textContent = fmtNum(today);
  $("stat-total").textContent = fmtNum(total);
  $("last-refresh").textContent = "updated " + relTime(Math.floor(Date.now() / 1000));
  $("empty-state").classList.toggle("hidden", dbs.length > 0);
}

// ── Detail ──────────────────────────────────────────────────────────

async function loadDetail(dbID) {
  let d;
  try {
    d = await api("/v1/monitor/db/" + encodeURIComponent(dbID));
  } catch (e) {
    $("detail-name").textContent = "Error";
    $("detail-id").textContent = e.message;
    return;
  }

  $("detail-name").textContent = d.name;
  $("detail-id").textContent = d.db_id;
  $("detail-status").outerHTML = badgeHtml(d.status);
  $("detail-total").textContent = fmtNum(d.total_entries);
  $("detail-today").textContent = fmtNum(d.entries_today);
  $("detail-today-label").textContent =
    "Entries today · " + fmtBytes(d.bytes_today);
  $("detail-snap").textContent = d.snapshots || 0;
  $("detail-chunks").textContent = d.chunks || 0;

  const daily = seriesFor(d.daily || [], 14);
  $("chart-14d").innerHTML = barChart(daily, { w: 560, h: 60 });

  const et = $("entries-table").querySelector("tbody");
  et.innerHTML = "";
  if (!(d.recent_entries || []).length) {
    et.innerHTML = "<tr><td colspan='6' class='muted'>No WAL entries yet</td></tr>";
  } else {
    for (const e of d.recent_entries) {
      const tr = document.createElement("tr");
      tr.innerHTML =
        "<td class='mono'>" + e.lsn + "</td>" +
        "<td class='muted'>" + fmtTs(e.ts, e.received_at) + "</td>" +
        "<td>" + esc(e.op) + "</td>" +
        "<td class='mono'>" + esc(e.table_id) + "</td>" +
        "<td class='mono'>" + esc(e.pk) + "</td>" +
        "<td class='sql mono'>" + esc((e.sql || "").slice(0, 120)) + "</td>";
      et.appendChild(tr);
    }
  }

  const st = $("snap-table").querySelector("tbody");
  st.innerHTML = "";
  if (!(d.snapshots || []).length) {
    st.innerHTML = "<tr><td colspan='3' class='muted'>No snapshots yet</td></tr>";
  } else {
    for (const s of d.snapshots) {
      const tr = document.createElement("tr");
      tr.innerHTML =
        "<td class='mono'>" + (s.baseline_lsn || 0) + "</td>" +
        "<td class='mono'>" + esc(s.s3_key) + "</td>" +
        "<td class='muted'>" + (s.created_at ? new Date(s.created_at * 1000).toLocaleString() : "–") + "</td>";
      st.appendChild(tr);
    }
  }
}

// ── Polling ─────────────────────────────────────────────────────────

function startPolling() {
  stopPolling();
  state.pollTimer = setInterval(async () => {
    if (!state.token || $("view-dash").classList.contains("hidden")) return;
    try {
      if (state.dbID) {
        await loadDetail(state.dbID);
      } else {
        await loadSummary();
      }
    } catch (_) { /* 401 path handled inside api(); transient errors keep the last frame */ }
  }, 15000);
}

function stopPolling() {
  if (state.pollTimer) {
    clearInterval(state.pollTimer);
    state.pollTimer = null;
  }
}

// ── Events ──────────────────────────────────────────────────────────

$("login-form").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const email = $("login-email").value.trim();
  const password = $("login-password").value;
  $("login-error").classList.add("hidden");
  try {
    const res = await fetch("/v1/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, password }),
    });
    const body = await res.json();
    if (!res.ok) throw new Error(body.error || "login failed");
    setToken(body.token, email);
    showDash();
  } catch (e) {
    $("login-error").textContent = e.message;
    $("login-error").classList.remove("hidden");
  }
});

$("logout-btn").addEventListener("click", () => {
  setToken("");
  showLogin("");
});

$("back-btn").addEventListener("click", goOverview);

// ── Boot ────────────────────────────────────────────────────────────

(async function boot() {
  if (state.token) {
    try {
      await loadSummary();
      showDash();
      return;
    } catch (e) {
      setToken(""); // token rejected or expired
    }
  }
  showLogin("");
})();
