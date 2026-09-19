#!/usr/bin/env node
/* Arkilian install helper: fetch prebuilt N-API binary from the latest
 * GitHub release instead of bundling every platform in the npm tarball.
 * Zero deps (Node >=18 only). Falls back to node-gyp source build. */
"use strict";
const { spawnSync } = require("node:child_process");
const fs = require("node:fs");
const https = require("node:https");
const os = require("node:os");
const path = require("node:path");
const ROOT = path.resolve(__dirname);
const PREBUILDS = path.join(ROOT, "prebuilds");
const PKG = (() => { try { return require("./package.json"); } catch { return { version: "1.5.0" }; } })();
const VERSION = PKG.version || "1.5.0";
const REPO = process.env.ARKILIAN_REPO || "arkiliandb/Arkilian";
const log = (...a) => console.log("[arkilian]", ...a);
const warn = (...a) => console.warn("[arkilian]", ...a);

function isMuslLdd() {
  try {
    const r = spawnSync("ldd", ["--version"], { encoding: "utf8" });
    return /musl/i.test(String(r.stdout || "") + String(r.stderr || ""));
  } catch { return false; }
}
function detect() {
  const platform = process.env.npm_config_platform || os.platform();
  const arch = process.env.npm_config_arch || os.arch();
  let libc = process.env.LIBC || process.env.PREBUILD_LIBC || "";
  if (!libc && platform === "linux") {
    try {
      if (fs.existsSync("/etc/alpine-release")) libc = "musl";
      else if (isMuslLdd()) libc = "musl";
      else {
        const g = spawnSync("getconf", ["GNU_LIBC_VERSION"], { encoding: "utf8" });
        libc = (g.status === 0 && String(g.stdout || "").trim()) ? "glibc" : "glibc";
      }
    } catch { libc = "glibc"; }
    if (!libc) libc = "glibc";
  }
  const tuple = `${platform}-${arch}`;
  const fileTuple = (platform === "linux" && libc === "musl") ? `${tuple}-musl` : tuple;
  return { platform, arch, libc: libc || "glibc", tuple, fileTuple };
}
function hasBuild(tuple) {
  try {
    const d = path.join(PREBUILDS, tuple);
    const f = fs.readdirSync(d).filter((x) => x.endsWith(".node"));
    return f.length ? path.join(d, f[0]) : null;
  } catch { return null; }
}
function fetch(url, dest, n = 5) {
  if (/^file:/i.test(url)) {
    const p = new URL(url).pathname;
    return new Promise((resolve, reject) => {
      fs.copyFile(p, dest, (e) => (e ? reject(e) : resolve()));
    });
  }
  return new Promise((resolve, reject) => {
    const h = { "User-Agent": "arkilian-npm-installer", Accept: "application/octet-stream" };
    if (process.env.GITHUB_TOKEN) h.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;
    const req = https.get(url, { headers: h }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        if (n <= 0) { res.resume(); return reject(new Error("too many redirects")); }
        res.resume();
        return resolve(fetch(new URL(res.headers.location, url).toString(), dest, n - 1));
      }
      if (res.statusCode !== 200) { res.resume(); return reject(new Error(`HTTP ${res.statusCode} for ${url}`)); }
      const out = fs.createWriteStream(dest);
      res.pipe(out);
      out.on("finish", () => out.close(resolve));
      out.on("error", reject);
      res.on("error", reject);
    });
    req.on("error", reject);
    req.setTimeout(120000, () => req.destroy(new Error(`timeout: ${url}`)));
  });
}
function extract(tarball, fileTuple, tuple) {
  fs.mkdirSync(PREBUILDS, { recursive: true });
  const list = spawnSync("tar", ["tzf", tarball], { encoding: "utf8" });
  if (list.status !== 0) throw new Error("cannot list prebuild archive");
  const all = String(list.stdout || "").split("\n");
  let want = all.filter((l) => l.includes(`prebuilds/${fileTuple}/`) && l.endsWith(".node"));
  if (!want.length) want = all.filter((l) => l.includes(`prebuilds/${tuple}/`) && l.endsWith(".node"));
  if (!want.length) throw new Error(`no prebuild for ${fileTuple} in archive`);
  const r = spawnSync("tar", ["xzf", tarball, "-C", ROOT, ...want], { stdio: "inherit" });
  if (r.status !== 0) throw new Error("tar extract failed");
  if (fileTuple !== tuple) {
    const src = path.join(PREBUILDS, fileTuple), dst = path.join(PREBUILDS, tuple);
    try {
      if (fs.existsSync(src) && !fs.existsSync(dst)) fs.renameSync(src, dst);
      else if (fs.existsSync(src)) for (const f of fs.readdirSync(src)) fs.copyFileSync(path.join(src, f), path.join(dst, f));
    } catch { /* keep musl-suffixed dir */ }
  }
}
function copyLocal(dir, fileTuple, tuple) {
  for (const w of [fileTuple, tuple]) {
    const src = path.join(dir, "prebuilds", w);
    if (!fs.existsSync(src)) continue;
    const files = fs.readdirSync(src).filter((f) => f.endsWith(".node"));
    if (!files.length) continue;
    fs.mkdirSync(path.join(PREBUILDS, tuple), { recursive: true });
    for (const f of files) fs.copyFileSync(path.join(src, f), path.join(PREBUILDS, tuple, f));
    return true;
  }
  return false;
}
function buildSrc() {
  if (process.env.ARKILIAN_SKIP_BUILD === "1") throw new Error("source build skipped (ARKILIAN_SKIP_BUILD=1)");
  log("building from source (node-gyp rebuild) ...");
  const local = path.join(ROOT, "node_modules", ".bin", process.platform === "win32" ? "node-gyp.cmd" : "node-gyp");
  const r = fs.existsSync(local)
    ? spawnSync(process.execPath, [local, "rebuild"], { cwd: ROOT, stdio: "inherit" })
    : spawnSync("npx", ["--yes", "node-gyp", "rebuild"], { cwd: ROOT, stdio: "inherit", shell: process.platform === "win32" });
  if (r.status !== 0) throw new Error("node-gyp rebuild failed");
}
async function main() {
  const { tuple, fileTuple } = detect();
  log(`installing arkilian v${VERSION} for ${fileTuple} ...`);
  if (hasBuild(tuple) || hasBuild(fileTuple)) { log("prebuild already present"); return; }
  if (process.env.ARKILIAN_SKIP_DOWNLOAD === "1") { log("skip download, source build"); return buildSrc(); }
  if (process.env.ARKILIAN_PREBUILD_DIR) {
    if (copyLocal(process.env.ARKILIAN_PREBUILD_DIR, fileTuple, tuple)) { log(`copied prebuild from ${process.env.ARKILIAN_PREBUILD_DIR}`); return; }
    warn("ARKILIAN_PREBUILD_DIR has no matching prebuild; continuing");
  }
  const tag = process.env.ARKILIAN_PREBUILD_TAG || "latest";
  const urls = [];
  if (process.env.ARKILIAN_PREBUILD_URL) urls.push(process.env.ARKILIAN_PREBUILD_URL);
  else if (tag === "latest") urls.push(
    `https://github.com/${REPO}/releases/latest/download/arkilian-node-prebuilds-v${VERSION}.tar.gz`
  );
  else urls.push(
    `https://github.com/${REPO}/releases/download/${tag}/arkilian-node-prebuilds-v${VERSION}.tar.gz`,
    `https://github.com/${REPO}/releases/latest/download/arkilian-node-prebuilds-v${VERSION}.tar.gz`
  );
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "ark-pre-"));
  const tgz = path.join(tmp, "p.tar.gz");
  let ok = false, last = null;
  for (const u of urls) {
    try { log(`downloading ${u}`); await fetch(u, tgz); ok = true; break; }
    catch (e) { last = e; warn(String((e && e.message) || e)); }
  }
  try {
    if (!ok) throw last || new Error("download failed");
    extract(tgz, fileTuple, tuple);
    if (!hasBuild(tuple) && !hasBuild(fileTuple)) throw new Error(`no .node for ${fileTuple}`);
    log(`installed prebuild for ${fileTuple} from GitHub release`);
  } catch (e) {
    warn(`download failed (${(e && e.message) || e}); source-build fallback`);
    buildSrc();
  } finally { try { fs.rmSync(tmp, { recursive: true, force: true }); } catch {} }
}
main().catch((e) => { console.error("[arkilian] install failed:", (e && e.message) || e); process.exit(1); });
