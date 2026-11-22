#!/usr/bin/env node
// OpenCode AI OAuth and Service Manager for modelmesh.
// Supports OpenCode Console Device Code flow, credential management,
// token refresh, and background service lifecycle.
import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";
import { spawn, execSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { cacheDir, configFile, configDir as pathsConfigDir, atomicWrite } from "./cc-paths.mjs";

export const CLIENT_ID = process.env.MODELMESH_OPENCODE_CLIENT_ID || "opencode-cli";
export const CONSOLE_URL = process.env.MODELMESH_OPENCODE_CONSOLE_URL || "https://opencode.ai/console";
export const INFERENCE_URL = process.env.MODELMESH_OPENCODE_INFERENCE_URL || "https://opencode.ai/inference/openai/v1";
export const USER_AGENT = "opencode/latest/2.3.2/opencode";

export const FREE_MODELS = [
  { id: "deepseek-v4.1-flash-free", name: "DeepSeek V4.1 Flash Free (Reasoning)", contextTokens: 200000 },
  { id: "tidepool-alpha-free", name: "Tidepool Alpha Free", contextTokens: 200000 },
  { id: "hy4-flash-free", name: "Tencent Hy4 Flash Free", contextTokens: 200000 },
  { id: "flint-40b-free", name: "Flint 40B Free", contextTokens: 128000 },
  { id: "iceman-free", name: "Iceman Free", contextTokens: 128000 },
  { id: "nemotron-3.5-super-vl-free", name: "Nemotron 3.5 Super VL Free", contextTokens: 128000 },
  { id: "mimo-v2.6-flash-free", name: "MiMo V2.6 Flash Free", contextTokens: 200000 },
  { id: "beam-free", name: "Beam Free", contextTokens: 128000 },
  { id: "big-pickle", name: "Big Pickle", contextTokens: 128000 },
];

export function configDir() { return pathsConfigDir(); }
export function credsPath() { return path.join(configDir(), "opencode.json"); }

export function loadCredentials() {
  try {
    const raw = fs.readFileSync(credsPath(), "utf8");
    const creds = JSON.parse(raw);
    if (creds && creds.access_token) return creds;
  } catch (e) {}

  // Auto-detect existing OpenCode CLI credentials if modelmesh credentials do not exist yet
  try {
    const imported = detectNativeCredentials();
    if (imported) {
      saveCredentials(imported);
      return imported;
    }
  } catch (e) {}
  return null;
}

export function saveCredentials(creds) {
  atomicWrite(credsPath(), JSON.stringify(creds, null, 2) + "\n");
  return creds;
}

export function clearCredentials() {
  try { fs.unlinkSync(credsPath()); } catch (e) {}
}

export function detectNativeCredentials() {
  try {
    const home = process.env.HOME || "/data/data/com.termux/files/home";
    const dbPath = path.join(home, ".local/share/opencode/opencode.db");
    if (!fs.existsSync(dbPath)) return null;

    const opencodeBin = path.join(home, "opencode-native/opencode");
    const binToRun = fs.existsSync(opencodeBin) ? opencodeBin : "opencode";
    const raw = execSync(`"${binToRun}" auth export 2>/dev/null`, { timeout: 8000 }).toString();
    const idx = raw.indexOf("[");
    if (idx === -1) return null;
    const list = JSON.parse(raw.slice(idx));
    const active = list.find((c) => c.integrationID === "opencode" && c.value && c.value.access);
    if (!active) return null;

    const val = active.value;
    const meta = val.metadata || {};
    return {
      type: "oauth",
      access_token: val.access,
      refresh_token: val.refresh,
      expires_at: val.expires || (Date.now() + 30 * 86400 * 1000),
      email: meta.email || null,
      account_id: meta.accountID || null,
      org_id: meta.orgID || null,
      org_name: meta.orgName || active.label || null,
      server: meta.server || CONSOLE_URL,
      imported_from: "opencode-native"
    };
  } catch (e) {
    return null;
  }
}

export async function refreshToken(refresh_token, server = CONSOLE_URL, fetchImpl = fetch) {
  const url = `${server.replace(/\/+$/, "")}/auth/device/token`;
  const res = await fetchImpl(url, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      accept: "application/json",
      "user-agent": USER_AGENT,
    },
    body: JSON.stringify({
      grant_type: "refresh_token",
      refresh_token,
      client_id: CLIENT_ID,
    }),
  });

  if (!res.ok) {
    const body = (await res.text()).slice(0, 300);
    const err = new Error(`token refresh failed: HTTP ${res.status} ${body}`);
    err.status = res.status;
    err.invalidCredential = res.status === 400 || /invalid_grant|invalid_client/i.test(body);
    throw err;
  }

  return await res.json();
}

export async function ensureAccessToken(fetchImpl = fetch) {
  const creds = loadCredentials();
  if (!creds || !creds.access_token) return null;

  // Refresh 5 minutes before expiry
  const now = Date.now();
  if (creds.expires_at && now > creds.expires_at - 300000 && creds.refresh_token) {
    try {
      const refreshed = await refreshToken(creds.refresh_token, creds.server || CONSOLE_URL, fetchImpl);
      creds.access_token = refreshed.access_token;
      if (refreshed.refresh_token) creds.refresh_token = refreshed.refresh_token;
      if (refreshed.expires_in) creds.expires_at = Date.now() + refreshed.expires_in * 1000;
      if (refreshed.org_id) creds.org_id = refreshed.org_id;
      saveCredentials(creds);
    } catch (e) {
      if (e.invalidCredential) {
        console.error(new Date().toISOString(), "opencode: stored refresh token invalid/revoked; please re-login");
      }
    }
  }
  return creds.access_token;
}

export function readServiceInfo() {
  const home = process.env.HOME || "/data/data/com.termux/files/home";
  const stateService = path.join(home, ".local/state/opencode/service.json");
  const configService = path.join(home, ".config/opencode/service.json");

  let info = null;
  if (fs.existsSync(stateService)) {
    try { info = JSON.parse(fs.readFileSync(stateService, "utf8")); } catch (e) {}
  }
  if (!info && fs.existsSync(configService)) {
    try { info = JSON.parse(fs.readFileSync(configService, "utf8")); } catch (e) {}
  }
  return info;
}

export async function isServiceAlive(url, password, fetchImpl = fetch) {
  if (!url) return false;
  try {
    const auth = password ? "Basic " + Buffer.from("opencode:" + password).toString("base64") : undefined;
    const res = await fetchImpl(`${url.replace(/\/+$/, "")}/api/info`, {
      headers: auth ? { authorization: auth } : {},
      signal: AbortSignal.timeout(3000),
    });
    return res.ok;
  } catch (e) {
    return false;
  }
}

export async function ensureService({ log = () => {} } = {}) {
  let info = readServiceInfo();
  if (info && info.url && (await isServiceAlive(info.url, info.password))) {
    return info;
  }

  log("Starting OpenCode background service...");
  const home = process.env.HOME || "/data/data/com.termux/files/home";
  const opencodeBin = path.join(home, "opencode-native/opencode");
  const binToRun = fs.existsSync(opencodeBin) ? opencodeBin : "opencode";

  const child = spawn(binToRun, ["serve", "--service"], {
    detached: true,
    stdio: "ignore",
    env: { ...process.env, AGY_PROXY_PORT: process.env.AGY_PROXY_PORT || "8975" }
  });
  child.unref();

  // Wait for service.json to be created and answer
  const deadline = Date.now() + 8000;
  while (Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 400));
    info = readServiceInfo();
    if (info && info.url && (await isServiceAlive(info.url, info.password))) {
      log(`OpenCode background service ready at ${info.url}`);
      return info;
    }
  }

  throw new Error("Failed to start OpenCode background service");
}

export async function login({ log = console.log, server = CONSOLE_URL, fetchImpl = fetch } = {}) {
  // Check if native OpenCode has active credentials first
  const existing = detectNativeCredentials();
  if (existing && existing.access_token) {
    log(`Found existing OpenCode credentials for ${existing.email || "account"} (org: ${existing.org_name || existing.org_id || "Personal"}).`);
    saveCredentials(existing);
    log(`Saved OpenCode credentials to ${credsPath()}`);
    return existing;
  }

  // Device Code OAuth Flow
  const srv = server.replace(/\/+$/, "");
  const codeRes = await fetchImpl(`${srv}/auth/device/code`, {
    method: "POST",
    headers: { "content-type": "application/json", accept: "application/json", "user-agent": USER_AGENT },
    body: JSON.stringify({ client_id: CLIENT_ID, supports_org_scope: true }),
  });

  if (!codeRes.ok) {
    throw new Error(`Failed to initiate device code login: HTTP ${codeRes.status} ${(await codeRes.text()).slice(0, 200)}`);
  }

  const codeData = await codeRes.json();
  const { device_code, user_code, verification_uri_complete, interval = 5, expires_in = 900 } = codeData;
  const authUrl = verification_uri_complete
    ? (verification_uri_complete.startsWith("http") ? verification_uri_complete : new URL(verification_uri_complete, "https://opencode.ai").href)
    : `https://opencode.ai/console/device?user_code=${user_code}`;

  log("");
  log("============================================================");
  log("  OpenCode Console Authentication");
  log("============================================================");
  log(`  1. Visit: ${authUrl}`);
  log(`  2. Confirm code: ${user_code}`);
  log("============================================================");
  log("Waiting for approval in browser...");

  const pollInterval = Math.max(interval, 2) * 1000;
  const deadline = Date.now() + expires_in * 1000;

  while (Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, pollInterval));
    const tokenRes = await fetchImpl(`${srv}/auth/device/token`, {
      method: "POST",
      headers: { "content-type": "application/json", accept: "application/json", "user-agent": USER_AGENT },
      body: JSON.stringify({
        grant_type: "urn:ietf:params:oauth:grant-type:device_code",
        device_code,
        client_id: CLIENT_ID,
      }),
    });

    const tokenData = await tokenRes.json();
    if (tokenData.access_token) {
      log("Authorization approved!");
      // Fetch user profile and orgs
      let email = null, accountID = null, orgID = tokenData.org_id || null, orgName = null;
      try {
        const [uRes, oRes] = await Promise.all([
          fetchImpl(`${srv}/api/user`, { headers: { authorization: `Bearer ${tokenData.access_token}`, "user-agent": USER_AGENT } }),
          fetchImpl(`${srv}/api/orgs`, { headers: { authorization: `Bearer ${tokenData.access_token}`, "user-agent": USER_AGENT } }),
        ]);
        if (uRes.ok) {
          const u = await uRes.json();
          email = u.email; accountID = u.id;
        }
        if (oRes.ok) {
          const orgs = await oRes.json();
          const target = orgID ? orgs.find((o) => o.id === orgID) : orgs[0];
          if (target) { orgID = target.id; orgName = target.name; }
        }
      } catch (e) {}

      const creds = {
        type: "oauth",
        access_token: tokenData.access_token,
        refresh_token: tokenData.refresh_token,
        expires_at: Date.now() + (tokenData.expires_in || 3600) * 1000,
        email,
        account_id: accountID,
        org_id: orgID,
        org_name: orgName,
        server: srv,
      };

      saveCredentials(creds);
      log(`Logged in as ${email || "user"}${orgName ? ` (org: ${orgName})` : ""}`);
      return creds;
    }

    if (tokenData.error === "authorization_pending") continue;
    if (tokenData.error === "slow_down") {
      await new Promise((r) => setTimeout(r, 5000));
      continue;
    }
    throw new Error(`Device authorization failed: ${tokenData.error || JSON.stringify(tokenData)}`);
  }

  throw new Error("Device code authorization timed out");
}

export async function fetchAvailableModels({ freeOnly = false, timeoutMs = 8000 } = {}) {
  // 1. Fetch dynamically from local OpenCode background service
  try {
    const svc = readServiceInfo();
    if (svc && svc.url) {
      const auth = svc.password ? "Basic " + Buffer.from("opencode:" + svc.password).toString("base64") : undefined;
      const res = await fetch(`${svc.url.replace(/\/+$/, "")}/api/model`, {
        headers: auth ? { authorization: auth } : {},
        signal: AbortSignal.timeout(timeoutMs),
      });
      if (res.ok) {
        const json = await res.json();
        const list = (json.data || json).filter((m) => m.providerID === "opencode" || !m.providerID);
        if (list.length > 0) {
          const out = [];
          for (const m of list) {
            const isFree = m.id.endsWith("-free") || (Array.isArray(m.cost) && m.cost.every((c) => (!c.input || c.input === 0) && (!c.output || c.output === 0)));
            if (freeOnly && !isFree) continue;
            out.push({
              id: m.id || m.modelID,
              name: m.name || m.id,
              contextTokens: (m.limit && m.limit.context) || 200000,
              free: isFree,
            });
          }
          if (out.length > 0) return out;
        }
      }
    }
  } catch (e) {}

  // 2. Fetch dynamically from ~/.cache/opencode/models.json
  try {
    const home = process.env.HOME || "/data/data/com.termux/files/home";
    const cachePath = path.join(home, ".cache/opencode/models.json");
    if (fs.existsSync(cachePath)) {
      const data = JSON.parse(fs.readFileSync(cachePath, "utf8"));
      const opencodeProv = data.opencode || (data.providers && data.providers.opencode);
      if (opencodeProv && opencodeProv.models) {
        const out = [];
        for (const [id, m] of Object.entries(opencodeProv.models)) {
          const isFree = id.endsWith("-free") || (m.cost && !m.cost.input && !m.cost.output);
          if (freeOnly && !isFree) continue;
          out.push({
            id,
            name: m.name || id,
            contextTokens: (m.limit && m.limit.context) || 200000,
            free: isFree,
          });
        }
        if (out.length > 0) return out;
      }
    }
  } catch (e) {}

  // 3. Fallback
  return FREE_MODELS;
}

export async function status({ log = console.log } = {}) {
  const creds = loadCredentials();
  if (!creds || !creds.access_token) {
    log("OpenCode is not logged in. Run: modelmesh opencode login");
    return;
  }

  const exp = creds.expires_at ? new Date(creds.expires_at).toLocaleString() : "unknown";
  log("OpenCode Authentication Status:");
  log(`  Account:   ${creds.email || creds.account_id || "authenticated"}`);
  if (creds.org_name || creds.org_id) log(`  Org:       ${creds.org_name || ""} (${creds.org_id || ""})`);
  log(`  Server:    ${creds.server || CONSOLE_URL}`);
  log(`  Expires:   ${exp}`);

  const svc = readServiceInfo();
  if (svc && (await isServiceAlive(svc.url, svc.password))) {
    log(`  Service:   ● Running at ${svc.url}`);
  } else {
    log("  Service:   ○ Not running (will start on first request)");
  }
}

// CLI dispatcher
if (process.argv[1] && path.resolve(process.argv[1]) === path.resolve(fileURLToPath(import.meta.url))) {
  const cmd = process.argv[2] || "status";
  (async () => {
    try {
      if (cmd === "login") await login();
      else if (cmd === "status") await status();
      else if (cmd === "logout") { clearCredentials(); console.log("Logged out of OpenCode."); }
      else if (cmd === "models") {
        const showAll = process.argv.includes("--all");
        const list = await fetchAvailableModels({ freeOnly: !showAll });
        console.log(`OpenCode ${showAll ? "All" : "Free"} Models (dynamically fetched, ${list.length} available):`);
        for (const m of list) {
          const badge = m.free ? " (free)" : "";
          console.log(`  - ${m.id.padEnd(30)} ${m.name}${badge} (${Math.round(m.contextTokens / 1000)}k ctx)`);
        }
      } else {
        console.log("usage: cc-opencode.mjs [login|status|logout|models]");
      }
    } catch (e) {
      console.error("Error:", e.message);
      process.exit(1);
    }
  })();
}
