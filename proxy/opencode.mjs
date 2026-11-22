// OpenCode AI adapter for the local modelmesh gateway.
// Translates Anthropic Messages <-> OpenCode session/inference and streams SSE.
import crypto from "node:crypto";
import http from "node:http";
import { ensureAccessToken, ensureService, loadCredentials, FREE_MODELS } from "../bin/cc-opencode.mjs";

const DEFAULT_MODEL = "deepseek-v4.1-flash-free";
const MODEL_ALIASES = {
  "deepseek-flash": "deepseek-v4.1-flash-free",
  "deepseek": "deepseek-v4.1-flash-free",
  "tidepool": "tidepool-alpha-free",
  "hy4": "hy4-flash-free",
  "tencent-hy4": "hy4-flash-free",
  "flint": "flint-40b-free",
  "iceman": "iceman-free",
  "nemotron": "nemotron-3.5-super-vl-free",
  "mimo": "mimo-v2.6-flash-free",
  "beam": "beam-free",
};

export function resolveModel(id) {
  const clean = String(id || "").trim();
  if (FREE_MODELS.some((m) => m.id === clean)) return clean;
  if (MODEL_ALIASES[clean]) return MODEL_ALIASES[clean];
  const matched = FREE_MODELS.find((m) => clean.includes(m.id.replace(/-free$/, "")) || clean.includes(m.id));
  if (matched) return matched.id;
  return DEFAULT_MODEL;
}

export function formatAnthropicToPrompt(body) {
  let out = "";
  if (body.system) {
    const sys = typeof body.system === "string" ? body.system : (body.system || []).map((s) => s.text || "").join("\n");
    if (sys.trim()) out += `[System Instructions]\n${sys.trim()}\n\n`;
  }

  const msgs = body.messages || [];
  for (let i = 0; i < msgs.length; i++) {
    const m = msgs[i];
    const role = m.role === "assistant" ? "Assistant" : "User";
    let text = "";
    if (typeof m.content === "string") {
      text = m.content;
    } else if (Array.isArray(m.content)) {
      text = m.content
        .map((p) => {
          if (p.type === "text") return p.text || "";
          if (p.type === "tool_use") return `[Tool Use: ${p.name}(${JSON.stringify(p.input || {})})]`;
          if (p.type === "tool_result") return `[Tool Result: ${typeof p.content === "string" ? p.content : JSON.stringify(p.content || "")}]`;
          return "";
        })
        .filter(Boolean)
        .join("\n");
    }

    if (i === msgs.length - 1 && m.role === "user") {
      // Last user turn
      if (out.length > 0) out += `[${role}]: ${text}`;
      else out = text;
    } else {
      out += `[${role}]: ${text}\n\n`;
    }
  }
  return out.trim();
}

export async function proxyOpencode(req, res, route, body, { anthropicError = () => {}, sink = null } = {}) {
  const started = Date.now();
  const wantStream = !!body.stream;
  const modelId = resolveModel(route.model);

  let settled = false;
  const fail = (status, type, message) => {
    if (settled) return;
    settled = true;
    if (sink) {
      sink("error", { type: "error", error: { type, message } });
      return;
    }
    if (!res || res.writableEnded) return;
    if (!res.headersSent) {
      anthropicError(res, status, type, message);
    } else {
      try {
        res.write("event: error\ndata: " + JSON.stringify({ type: "error", error: { type, message } }) + "\n\n");
        res.end();
      } catch (e) {}
    }
  };

  // 1. Ensure OAuth credentials exist
  const token = await ensureAccessToken();
  if (!token) {
    return fail(401, "authentication_error", "OpenCode is not logged in on this device - run: modelmesh opencode login");
  }

  // 2. Ensure background service is running
  let svc;
  try {
    svc = await ensureService();
  } catch (e) {
    return fail(502, "api_error", "Failed to start OpenCode background service: " + e.message);
  }

  const auth = svc.password ? "Basic " + Buffer.from("opencode:" + svc.password).toString("base64") : undefined;
  const svcHeaders = {
    "content-type": "application/json",
    accept: "application/json",
    ...(auth ? { authorization: auth } : {}),
  };

  // 3. Create a session for this prompt
  let session;
  try {
    const sRes = await fetch(`${svc.url.replace(/\/+$/, "")}/api/session`, {
      method: "POST",
      headers: svcHeaders,
      body: JSON.stringify({
        title: "modelmesh-" + Date.now(),
        model: { providerID: "opencode", id: modelId },
      }),
      signal: AbortSignal.timeout(10000),
    });
    if (!sRes.ok) {
      const errText = (await sRes.text()).slice(0, 300);
      return fail(sRes.status, "api_error", `OpenCode session creation failed: HTTP ${sRes.status} ${errText}`);
    }
    const sJson = await sRes.json();
    session = sJson.data;
  } catch (e) {
    return fail(502, "api_error", "OpenCode session service unreachable: " + e.message);
  }

  const promptText = formatAnthropicToPrompt(body);
  if (!promptText) {
    return fail(400, "invalid_request_error", "messages must not be empty");
  }

  // Helper to send SSE events
  let startedSse = false;
  const sse = (event, data) => {
    if (sink) { sink(event, data); return; }
    if (!wantStream || !res) return;
    try {
      res.write("event: " + event + "\ndata: " + JSON.stringify(data) + "\n\n");
    } catch (e) {}
  };

  const startStream = () => {
    if (startedSse) return;
    startedSse = true;
    if (sink || !wantStream || !res) return;
    res.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
    });
    if (typeof res.flushHeaders === "function") res.flushHeaders();
    sse("message_start", {
      type: "message_start",
      message: {
        id: "msg_" + Date.now(),
        type: "message",
        role: "assistant",
        model: route.model,
        content: [],
        stop_reason: null,
        stop_sequence: null,
        usage: { input_tokens: 0, output_tokens: 0 },
      },
    });
  };

  // 4. Stream response via OpenCode SSE (/api/event)
  return new Promise((resolve) => {
    let blockIndex = 0;
    let textBlockStarted = false;
    let thinkingBlockStarted = false;
    let sawContent = false;
    let fullText = "";
    let usage = { input_tokens: 0, output_tokens: 0 };

    let timer = null;
    const cleanup = () => {
      if (timer) clearTimeout(timer);
      try {
        fetch(`${svc.url.replace(/\/+$/, "")}/api/session/${encodeURIComponent(session.id)}`, {
          method: "DELETE",
          headers: svcHeaders,
        }).catch(() => {});
      } catch (e) {}
    };

    const eventReq = http.request(`${svc.url.replace(/\/+$/, "")}/api/event`, {
      headers: auth ? { authorization: auth } : {},
    }, (upstreamRes) => {
      let buf = "";
      upstreamRes.on("data", (chunk) => {
        buf += chunk.toString();
        let nl;
        while ((nl = buf.indexOf("\n")) >= 0) {
          const line = buf.slice(0, nl).replace(/\r$/, "");
          buf = buf.slice(nl + 1);
          if (!line.startsWith("data:")) continue;
          const dataStr = line.slice(5).trim();
          if (!dataStr) continue;
          let ev;
          try { ev = JSON.parse(dataStr); } catch (e) { continue; }
          const d = ev.data || ev;
          if (d.sessionID !== session.id) continue;

          // Thinking / Reasoning delta
          if (ev.type === "session.thought.delta" || ev.type === "session.reasoning.delta" || d.reasoning) {
            const thought = d.reasoning || d.delta || "";
            if (thought) {
              startStream();
              if (!thinkingBlockStarted) {
                thinkingBlockStarted = true;
                sse("content_block_start", {
                  type: "content_block_start",
                  index: blockIndex,
                  content_block: { type: "thinking", thinking: "" },
                });
              }
              sse("content_block_delta", {
                type: "content_block_delta",
                index: blockIndex,
                delta: { type: "thinking_delta", thinking: thought },
              });
            }
          }

          // Text delta
          if (ev.type === "session.text.delta" && typeof d.delta === "string") {
            startStream();
            if (thinkingBlockStarted) {
              sse("content_block_stop", { type: "content_block_stop", index: blockIndex });
              thinkingBlockStarted = false;
              blockIndex++;
            }
            if (!textBlockStarted) {
              textBlockStarted = true;
              sse("content_block_start", {
                type: "content_block_start",
                index: blockIndex,
                content_block: { type: "text", text: "" },
              });
            }
            sse("content_block_delta", {
              type: "content_block_delta",
              index: blockIndex,
              delta: { type: "text_delta", text: d.delta },
            });
            fullText += d.delta;
            sawContent = true;
          }

          // Step finished
          if (ev.type === "session.step.ended" || ev.type === "session.execution.succeeded") {
            if (settled) return;
            settled = true;
            if (d.tokens) {
              usage.input_tokens = d.tokens.input || usage.input_tokens;
              usage.output_tokens = d.tokens.output || usage.output_tokens;
            }
            if (textBlockStarted || thinkingBlockStarted) {
              sse("content_block_stop", { type: "content_block_stop", index: blockIndex });
              textBlockStarted = false;
              thinkingBlockStarted = false;
            }

            if (wantStream) {
              sse("message_delta", {
                type: "message_delta",
                delta: { stop_reason: "end_turn", stop_sequence: null },
                usage: { output_tokens: usage.output_tokens },
              });
              sse("message_stop", { type: "message_stop" });
              if (res) res.end();
            } else if (res && !res.headersSent) {
              res.writeHead(200, { "content-type": "application/json" });
              res.end(JSON.stringify({
                id: "msg_" + Date.now(),
                type: "message",
                role: "assistant",
                model: route.model,
                content: [{ type: "text", text: fullText }],
                stop_reason: "end_turn",
                stop_sequence: null,
                usage,
              }));
            }

            try { eventReq.destroy(); } catch (e) {}
            cleanup();
            resolve();
          }

          // Execution or step failed
          if (ev.type === "session.step.failed" || ev.type === "session.execution.failed" || d.outcome === "failed") {
            if (settled) return;
            const err = d.error || (typeof d === "object" ? d : {});
            const isRateLimit = err.status === 429 || err.type === "provider.free-limit" || /free usage limit/i.test(err.message || "");
            const status = isRateLimit ? 429 : (err.status || 502);
            const errType = isRateLimit ? "rate_limit_error" : "api_error";
            let retryHint = "";
            if (err.retryAfterMs) {
              const hours = (err.retryAfterMs / 3600000).toFixed(1);
              retryHint = ` (resets in ~${hours}h)`;
              if (res && !res.headersSent && typeof res.setHeader === "function") {
                try { res.setHeader("retry-after", Math.ceil(err.retryAfterMs / 1000)); } catch (e) {}
              }
            }
            const msg = isRateLimit
              ? `OpenCode free usage limit reached${retryHint}: ${err.message || "Free models limit reached"}. Switch models using /model or subscribe at https://opencode.ai/go.`
              : (err.message || "OpenCode execution failed");
            try { eventReq.destroy(); } catch (e) {}
            cleanup();
            fail(status, errType, msg);
            resolve();
            return;
          }
        }
      });
    });

    eventReq.on("error", (e) => {
      fail(502, "api_error", "OpenCode event stream connection lost: " + e.message);
      cleanup();
      resolve();
    });

    eventReq.end();

    // 5. Send the prompt to the session
    fetch(`${svc.url.replace(/\/+$/, "")}/api/session/${encodeURIComponent(session.id)}/prompt`, {
      method: "POST",
      headers: svcHeaders,
      body: JSON.stringify({ text: promptText }),
    }).catch((e) => {
      fail(502, "api_error", "Failed to send prompt to OpenCode: " + e.message);
      try { eventReq.destroy(); } catch (err) {}
      cleanup();
      resolve();
    });

    // Timeout safety: 120s max
    timer = setTimeout(() => {
      if (!settled) {
        fail(504, "timeout", "OpenCode response timed out after 120s");
        try { eventReq.destroy(); } catch (e) {}
        cleanup();
        resolve();
      }
    }, 120000);
  });
}
