const http = require("node:http");
const { randomUUID } = require("node:crypto");
const { spawn } = require("node:child_process");

const agentName = process.env.AGENT_NAME || "openclaw";
const environmentName = process.env.ENVIRONMENT_NAME || "local";
const port = Number(process.env.PORT || 8080);
const gatewayPort = process.env.OPENCLAW_GATEWAY_PORT || "19001";
const a2aToken = process.env.A2A_SHARED_TOKEN || process.env.OPENCLAW_GATEWAY_TOKEN || "";
const a2aUseDapr = (process.env.A2A_USE_DAPR || "false").toLowerCase() === "true";
const daprHttpPort = process.env.DAPR_HTTP_PORT || "3500";
const containerAppsDomain = process.env.CONTAINER_APP_ENV_DNS_SUFFIX || "";
const peerNames = parseCsv(process.env.A2A_PEER_NAMES || "");
const explicitPeers = parsePeerMap(process.env.A2A_PEERS || "");
const tasks = new Map();
const taskEvents = new Map();
const taskSubscribers = new Map();

console.log(`${agentName} agent starting OpenClaw Gateway on port ${gatewayPort}`);

let openclawReady = false;
let openclawExitCode = null;

const openclaw = spawn(
  "openclaw",
  ["gateway", "run", "--allow-unconfigured", "--bind", "auto", "--port", gatewayPort, "--force"],
  {
    stdio: "inherit",
    env: process.env,
  },
);

openclaw.on("spawn", () => {
  openclawReady = true;
  console.log(`${agentName} OpenClaw runtime started`);
});

openclaw.on("error", (error) => {
  openclawReady = false;
  console.error(`${agentName} failed to start OpenClaw runtime`, error);
  process.exit(1);
});

openclaw.on("exit", (code, signal) => {
  openclawReady = false;
  openclawExitCode = code ?? 1;
  console.error(`${agentName} OpenClaw runtime exited`, { code, signal });
  process.exit(openclawExitCode);
});

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);

    if (req.method === "GET" && url.pathname === "/health") {
      return sendJson(res, openclawHealth().healthy ? 200 : 503, {
        agent: agentName,
        status: openclawHealth().healthy ? "ok" : "starting",
      });
    }

    if (req.method === "GET" && url.pathname === "/.well-known/agent-card.json") {
      return sendJson(res, 200, buildAgentCard(req));
    }

    if (req.method === "GET" && url.pathname === "/a2a/peers") {
      if (!authorize(req)) return unauthorized(res);
      return sendJson(res, 200, { agent: agentName, peers: buildPeers() });
    }

    if (req.method === "GET" && url.pathname === "/tasks") {
      if (!authorize(req)) return unauthorized(res);
      return sendJson(res, 200, { tasks: Array.from(tasks.values()) });
    }

    const taskMatch = url.pathname.match(/^\/tasks\/([^/]+)(?:\/cancel)?$/);
    if (taskMatch) {
      if (!authorize(req)) return unauthorized(res);
      const taskId = decodeURIComponent(taskMatch[1]);

      if (req.method === "GET" && !url.pathname.endsWith("/cancel")) {
        return sendJson(res, tasks.has(taskId) ? 200 : 404, tasks.get(taskId) || {
          error: { code: "task_not_found", message: `Task ${taskId} was not found.` },
        });
      }

      if (req.method === "POST" && url.pathname.endsWith("/cancel")) {
        return sendJson(res, 200, cancelTask(taskId));
      }
    }

    const taskEventsMatch = url.pathname.match(/^\/tasks\/([^/]+)\/events$/);
    if (taskEventsMatch && req.method === "GET") {
      if (!authorize(req)) return unauthorized(res);
      return streamTaskEvents(decodeURIComponent(taskEventsMatch[1]), req, res);
    }

    if (req.method === "POST" && (url.pathname === "/message:send" || url.pathname === "/a2a/message:send")) {
      if (!authorize(req)) return unauthorized(res);
      const body = await readJson(req);
      return sendJson(res, 200, await handleMessageSend(body));
    }

    if (req.method === "POST" && (url.pathname === "/message:stream" || url.pathname === "/a2a/message:stream")) {
      if (!authorize(req)) return unauthorized(res);
      const body = await readJson(req);
      return handleMessageStream(body, req, res);
    }

    if (req.method === "POST" && url.pathname === "/a2a") {
      if (!authorize(req)) return unauthorized(res);
      const rpc = await readJson(req);
      return sendJson(res, 200, await handleJsonRpc(rpc));
    }

    if (req.method === "GET" && url.pathname === "/") {
      return sendJson(res, 200, {
        agent: agentName,
        environment: environmentName,
        runtime: "openclaw",
        a2a: "/.well-known/agent-card.json",
      });
    }

    sendJson(res, 404, { error: { code: "not_found", message: "Route not found." } });
  } catch (error) {
    console.error(`${agentName} request failed`, error);
    sendJson(res, 500, { error: { code: "internal_error", message: error.message } });
  }
});

server.listen(port, "0.0.0.0", () => {
  console.log(`${agentName} health and A2A endpoint listening on ${port}`);
});

process.on("SIGTERM", () => {
  server.close();
  openclaw.kill("SIGTERM");
});

function buildAgentCard(req) {
  const baseUrl = publicBaseUrl(req);
  const peers = buildPeers();

  return {
    name: agentName,
    description: `${agentName} OpenClaw agent running in ${environmentName}.`,
    url: baseUrl,
    version: "0.1.0",
    protocolVersion: "0.3.0",
    preferredTransport: "HTTP+JSON",
    capabilities: {
      streaming: true,
      pushNotifications: false,
      stateTransitionHistory: true,
    },
    securitySchemes: {
      bearer: {
        type: "http",
        scheme: "bearer",
        description: "Use the A2A shared token configured as a Container App secret.",
      },
    },
    security: a2aToken ? [{ bearer: [] }] : [],
    defaultInputModes: ["text/plain", "application/json"],
    defaultOutputModes: ["text/plain", "application/json"],
    skills: [
      {
        id: "openclaw-message-routing",
        name: "OpenClaw message routing",
        description: "Accepts A2A messages and can forward messages to configured peer Container Apps.",
        tags: ["a2a", "openclaw", "container-apps"],
        examples: ["Send a task to analyst", "Forward this message to hermes"],
        inputModes: ["text/plain", "application/json"],
        outputModes: ["text/plain", "application/json"],
      },
    ],
    supportsAuthenticatedExtendedCard: false,
    endpoints: {
      messageSend: `${baseUrl}/message:send`,
      messageStream: `${baseUrl}/message:stream`,
      jsonRpc: `${baseUrl}/a2a`,
      tasks: `${baseUrl}/tasks`,
    },
    peers,
  };
}

async function handleMessageSend(body) {
  const params = body.params || body;
  const message = normalizeMessage(params.message || params);
  const metadata = params.metadata || message.metadata || {};
  const targetAgent = metadata.targetAgent || metadata.target_agent || params.targetAgent;

  if (targetAgent && targetAgent !== agentName) {
    return forwardToPeer(targetAgent, body);
  }

  const task = createTask(message, metadata);
  tasks.set(task.id, task);
  return task;
}

async function handleJsonRpc(rpc) {
  const id = rpc.id ?? null;

  try {
    if (rpc.method === "message/send" || rpc.method === "message:send") {
      return { jsonrpc: "2.0", id, result: await handleMessageSend(rpc.params || {}) };
    }

    if (rpc.method === "message/stream" || rpc.method === "message:stream") {
      return {
        jsonrpc: "2.0",
        id,
        error: {
          code: -32002,
          message: "Use POST /message:stream for Server-Sent Events streaming.",
        },
      };
    }

    if (rpc.method === "tasks/get") {
      const taskId = rpc.params?.id || rpc.params?.taskId;
      const task = tasks.get(taskId);
      if (!task) return rpcError(id, -32001, `Task ${taskId} was not found.`);
      return { jsonrpc: "2.0", id, result: task };
    }

    if (rpc.method === "tasks/cancel") {
      const taskId = rpc.params?.id || rpc.params?.taskId;
      return { jsonrpc: "2.0", id, result: cancelTask(taskId) };
    }

    return rpcError(id, -32601, `Method ${rpc.method} is not supported.`);
  } catch (error) {
    return rpcError(id, -32000, error.message);
  }
}

function createTask(message, metadata) {
  const now = new Date().toISOString();
  const taskId = metadata.taskId || randomUUID();
  const text = extractText(message);

  const task = {
    id: taskId,
    contextId: metadata.contextId || randomUUID(),
    kind: "task",
    status: {
      state: "completed",
      timestamp: now,
      message: {
        role: "agent",
        parts: [
          {
            kind: "text",
            text: `${agentName} accepted the message${text ? `: ${text}` : "."}`,
          },
        ],
      },
    },
    history: [
      {
        role: "user",
        parts: message.parts,
      },
    ],
    artifacts: [
      {
        artifactId: randomUUID(),
        name: "openclaw-routing-result",
        parts: [
          {
            kind: "data",
            data: {
              agent: agentName,
              environment: environmentName,
              receivedAt: now,
              openclawGatewayPort: gatewayPort,
            },
          },
        ],
      },
    ],
  };

  recordTaskEvent(taskId, "task.completed", task);
  return task;
}

function cancelTask(taskId) {
  const existing = tasks.get(taskId);

  if (!existing) {
    return {
      id: taskId,
      kind: "task",
      status: {
        state: "canceled",
        timestamp: new Date().toISOString(),
      },
    };
  }

  existing.status = {
    state: "canceled",
    timestamp: new Date().toISOString(),
  };
  tasks.set(taskId, existing);
  recordTaskEvent(taskId, "task.canceled", existing);
  return existing;
}

async function handleMessageStream(body, req, res) {
  const params = body.params || body;
  const message = normalizeMessage(params.message || params);
  const metadata = params.metadata || message.metadata || {};
  const targetAgent = metadata.targetAgent || metadata.target_agent || params.targetAgent;

  if (targetAgent && targetAgent !== agentName) {
    return streamFromPeer(targetAgent, body, req, res);
  }

  startSse(res);

  const taskId = metadata.taskId || randomUUID();
  const contextId = metadata.contextId || randomUUID();
  const now = new Date().toISOString();
  const text = extractText(message);
  const submittedTask = {
    id: taskId,
    contextId,
    kind: "task",
    status: {
      state: "submitted",
      timestamp: now,
    },
    history: [
      {
        role: "user",
        parts: message.parts,
      },
    ],
    artifacts: [],
  };

  tasks.set(taskId, submittedTask);
  emitTaskUpdate(res, taskId, "task.submitted", submittedTask);

  const workingTask = {
    ...submittedTask,
    status: {
      state: "working",
      timestamp: new Date().toISOString(),
      message: {
        role: "agent",
        parts: [
          {
            kind: "text",
            text: `${agentName} is processing the message.`,
          },
        ],
      },
    },
  };

  tasks.set(taskId, workingTask);
  emitTaskUpdate(res, taskId, "task.working", workingTask);

  const completedTask = {
    ...workingTask,
    status: {
      state: "completed",
      timestamp: new Date().toISOString(),
      message: {
        role: "agent",
        parts: [
          {
            kind: "text",
            text: `${agentName} accepted the message${text ? `: ${text}` : "."}`,
          },
        ],
      },
    },
    artifacts: [
      {
        artifactId: randomUUID(),
        name: "openclaw-routing-result",
        parts: [
          {
            kind: "data",
            data: {
              agent: agentName,
              environment: environmentName,
              receivedAt: new Date().toISOString(),
              openclawGatewayPort: gatewayPort,
              streamed: true,
            },
          },
        ],
      },
    ],
  };

  tasks.set(taskId, completedTask);
  emitTaskUpdate(res, taskId, "task.completed", completedTask);
  res.end();
}

async function streamFromPeer(targetAgent, body, req, res) {
  const peers = buildPeers();
  const peer = peers.find((item) => item.name === targetAgent || item.appId === targetAgent);

  if (!peer) {
    startSse(res);
    writeSse(res, "error", {
      error: { code: "peer_not_configured", message: `Peer ${targetAgent} is not configured.` },
    });
    return res.end();
  }

  const response = await fetch(peer.messageStreamUrl, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(a2aToken ? { authorization: `Bearer ${a2aToken}` } : {}),
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    startSse(res);
    writeSse(res, "error", {
      error: { code: "peer_stream_failed", message: `Peer ${targetAgent} returned HTTP ${response.status}.` },
    });
    return res.end();
  }

  res.writeHead(200, sseHeaders());
  const reader = response.body.getReader();

  while (!res.destroyed && !res.writableEnded) {
    const { value, done } = await reader.read();
    if (done) break;
    res.write(Buffer.from(value));
  }

  res.end();
}

function streamTaskEvents(taskId, req, res) {
  startSse(res);

  for (const event of taskEvents.get(taskId) || []) {
    writeSse(res, event.event, event.data);
  }

  const subscribers = taskSubscribers.get(taskId) || new Set();
  subscribers.add(res);
  taskSubscribers.set(taskId, subscribers);

  req.on("close", () => {
    subscribers.delete(res);
    if (subscribers.size === 0) taskSubscribers.delete(taskId);
  });
}

function emitTaskUpdate(res, taskId, event, task) {
  recordTaskEvent(taskId, event, task);
  writeSse(res, event, task);
}

function recordTaskEvent(taskId, event, data) {
  const events = taskEvents.get(taskId) || [];
  events.push({
    event,
    data,
    recordedAt: new Date().toISOString(),
  });
  taskEvents.set(taskId, events.slice(-50));

  for (const subscriber of taskSubscribers.get(taskId) || []) {
    writeSse(subscriber, event, data);
  }
}

function startSse(res) {
  res.writeHead(200, sseHeaders());
  res.write(": connected\n\n");
}

function sseHeaders() {
  return {
    "content-type": "text/event-stream",
    "cache-control": "no-cache, no-transform",
    connection: "keep-alive",
    "x-accel-buffering": "no",
  };
}

function writeSse(res, event, data) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(data)}\n\n`);
}

async function forwardToPeer(targetAgent, body) {
  const peers = buildPeers();
  const peer = peers.find((item) => item.name === targetAgent || item.appId === targetAgent);

  if (!peer) {
    return createTask(normalizeMessage(body.params?.message || body.message || body), {
      error: `Peer ${targetAgent} is not configured.`,
    });
  }

  const response = await fetch(peer.messageSendUrl, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(a2aToken ? { authorization: `Bearer ${a2aToken}` } : {}),
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    throw new Error(`Peer ${targetAgent} returned HTTP ${response.status}.`);
  }

  return response.json();
}

function buildPeers() {
  const names = Array.from(new Set([...peerNames, ...Object.keys(explicitPeers)])).filter((name) => name && name !== agentName);

  return names.map((name) => {
    const appId = appIdFor(name);
    const baseUrl = explicitPeers[name] || peerBaseUrl(name, appId);

      return {
      name,
      appId,
      baseUrl,
      messageSendUrl: a2aUseDapr
        ? `http://localhost:${daprHttpPort}/v1.0/invoke/${appId}/method/message:send`
        : `${baseUrl}/message:send`,
      messageStreamUrl: a2aUseDapr
        ? `http://localhost:${daprHttpPort}/v1.0/invoke/${appId}/method/message:stream`
        : `${baseUrl}/message:stream`,
    };
  });
}

function peerBaseUrl(name, appId) {
  if (containerAppsDomain) return `https://${appId}.${containerAppsDomain}`;
  return `http://${appId}`;
}

function appIdFor(name) {
  if (name.endsWith(`-${environmentName}`)) return name;
  return environmentName === "local" ? name : `${name}-${environmentName}`;
}

function authorize(req) {
  if (!a2aToken) return true;
  const authorization = req.headers.authorization || "";
  return authorization === `Bearer ${a2aToken}`;
}

function unauthorized(res) {
  sendJson(res, 401, { error: { code: "unauthorized", message: "A valid bearer token is required." } });
}

function normalizeMessage(message) {
  if (typeof message === "string") {
    return {
      role: "user",
      parts: [{ kind: "text", text: message }],
      metadata: {},
    };
  }

  return {
    role: message.role || "user",
    parts: Array.isArray(message.parts) ? message.parts : [{ kind: "data", data: message }],
    metadata: message.metadata || {},
  };
}

function extractText(message) {
  return message.parts
    .filter((part) => part.kind === "text" || part.type === "text")
    .map((part) => part.text)
    .filter(Boolean)
    .join(" ");
}

function openclawHealth() {
  return {
    healthy: openclawReady && openclawExitCode === null,
  };
}

function publicBaseUrl(req) {
  const forwardedProto = req.headers["x-forwarded-proto"];
  const host = req.headers.host || `localhost:${port}`;
  const proto = Array.isArray(forwardedProto)
    ? forwardedProto[0]
    : forwardedProto || (host.startsWith("localhost") || host.startsWith("127.0.0.1") ? "http" : "https");
  return `${proto}://${host}`;
}

function parseCsv(value) {
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function parsePeerMap(value) {
  const json = parseJsonObject(value);
  if (Object.keys(json).length > 0) return json;

  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean)
    .reduce((peers, item) => {
      const separator = item.indexOf("=");
      if (separator <= 0) return peers;

      const name = item.slice(0, separator).trim();
      const url = item.slice(separator + 1).trim();
      if (name && url) peers[name] = url;
      return peers;
    }, {});
}

function parseJsonObject(value) {
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

async function readJson(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  if (chunks.length === 0) return {};
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, { "content-type": "application/json" });
  res.end(JSON.stringify(payload));
}

function rpcError(id, code, message) {
  return {
    jsonrpc: "2.0",
    id,
    error: { code, message },
  };
}
