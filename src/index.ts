import { chmod, mkdir, readFile, rename, unlink, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { homedir } from "node:os";
import { Type } from "typebox";
import { v7 as uuidv7 } from "uuid";
import WebSocket from "ws";

type PiContext = {
  cwd?: string;
  sessionManager?: { getSessionFile?: () => string | Promise<string> };
  isStreaming?: boolean | (() => boolean);
  ui?: { notify?: (message: string, level?: string) => void };
};

type PiExtensionApi = {
  sendUserMessage: (content: string, options?: { deliverAs?: "steer" | "followUp" }) => void;
  registerCommand: (name: string, options: { description: string; handler: (args: string, ctx: PiContext) => Promise<void> }) => void;
  registerTool: (tool: {
    name: string;
    label: string;
    description: string;
    parameters: unknown;
    promptSnippet?: string;
    promptGuidelines?: string[];
    execute: (
      toolCallId: string,
      params: { to: string; body: string; message_id?: string },
      signal: AbortSignal | undefined,
      onUpdate: unknown,
      ctx: PiContext
    ) => Promise<{ content: Array<{ type: "text"; text: string }>; details: Record<string, unknown> }>;
  }) => void;
  on: (event: string, handler: (event: unknown, ctx: PiContext) => Promise<void> | void) => void;
};

type BindingState = {
  address: string;
  token: string;
  session_file: string;
  cwd: string;
  last_sequence: number;
  /** Entry injected as a user turn but not yet settled. Never more than one
   * may await settlement; the bookmark advances only after agent_settled. */
  pending_sequence: number | null;
  generation: number;
  session_id: string;
};

type InboxEntry = {
  sequence: number;
  event_id: string;
  type: string;
  occurred_at: string;
  payload: Record<string, unknown>;
};

const API_URL = (process.env.MYAGENT_API_URL || "https://myagent.to").replace(/\/$/, "");
const STATE_PATH = process.env.MYAGENT_PI_STATE || join(homedir(), ".config", "myagent", "pi-binding.json");
const PERMISSIONS = ["identity:read", "messages:write", "inbox:read", "inbox:bookmark", "contacts:read", "contacts:write"];
const REQUEST_TIMEOUT_MS = 20000;
let socket: WebSocket | null = null;
let draining = false;
let reconnectTimer: NodeJS.Timeout | null = null;
let reconnectAttempts = 0;
let activeCtx: PiContext | null = null;

function notify(ctx: PiContext, message: string): void {
  ctx.ui?.notify?.(message, "info");
}

async function api<T>(path: string, init: RequestInit = {}, bearer?: string): Promise<T> {
  const headers = new Headers(init.headers);
  headers.set("accept", "application/json");
  if (init.body && !headers.has("content-type")) headers.set("content-type", "application/json");
  if (bearer) headers.set("authorization", `Bearer ${bearer}`);
  const response = await fetch(`${API_URL}${path}`, { ...init, headers, signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS) });
  const body = await response.text();
  let parsed: unknown = null;
  try { parsed = body ? JSON.parse(body) : null; } catch { /* response errors are surfaced below */ }
  if (!response.ok) throw new Error(typeof parsed === "object" && parsed && "error" in parsed ? JSON.stringify(parsed) : `myagent HTTP ${response.status}`);
  return parsed as T;
}

async function readState(): Promise<BindingState | null> {
  try {
    const value = JSON.parse(await readFile(STATE_PATH, "utf8")) as BindingState;
    if (!value.token || !value.session_file || !value.cwd || !value.session_id) return null;
    if (typeof value.last_sequence !== "number") value.last_sequence = 0;
    if (value.pending_sequence !== null && typeof value.pending_sequence !== "number") value.pending_sequence = null;
    return value;
  } catch {
    return null;
  }
}

async function writeState(value: BindingState): Promise<void> {
  await mkdir(dirname(STATE_PATH), { recursive: true, mode: 0o700 });
  const temporary = `${STATE_PATH}.${process.pid}.tmp`;
  await writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, { encoding: "utf8", mode: 0o600 });
  await chmod(temporary, 0o600);
  await rename(temporary, STATE_PATH);
  await chmod(STATE_PATH, 0o600);
}

async function currentSession(ctx: PiContext): Promise<{ cwd: string; session_file: string }> {
  const cwd = ctx.cwd || process.cwd();
  const session_file = ctx.sessionManager?.getSessionFile ? String(await ctx.sessionManager.getSessionFile()) : "";
  if (!session_file) throw new Error("Pi did not expose the current session file.");
  return { cwd, session_file };
}

async function requireActiveBinding(ctx: PiContext): Promise<BindingState> {
  const state = await readState();
  if (!state) throw new Error("Run /myagent connect in this Pi session first.");
  const current = await currentSession(ctx);
  if (current.cwd !== state.cwd || current.session_file !== state.session_file) {
    throw new Error("This Pi session is not the session connected to myagent.");
  }
  const status = await api<{ binding: { provider: string; status: string; external_target_id: string | null; target_generation: number } | null }>("/v1/provider/status", {}, state.token);
  const binding = status.binding;
  if (!binding || binding.provider !== "pi" || binding.status !== "active" || binding.external_target_id !== state.session_id || binding.target_generation !== state.generation) {
    throw new Error("This Pi session is no longer the active myagent binding. Run /myagent connect again.");
  }
  return state;
}

function stopReconnect(): void {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  reconnectAttempts = 0;
}

function closeSocket(): void {
  try { socket?.close(1000, "myagent disconnected"); } catch { /* already closed */ }
  socket = null;
}

async function disconnect(ctx?: PiContext): Promise<void> {
  stopReconnect();
  const state = await readState();
  if (state) {
    try { await api("/v1/provider/disconnect", { method: "POST" }, state.token); } catch { /* local cleanup still proceeds */ }
  }
  closeSocket();
  try { await unlink(STATE_PATH); } catch { /* already absent */ }
  if (activeCtx === ctx || !ctx) activeCtx = null;
  if (ctx) notify(ctx, "myagent disconnected from this Pi session.");
}

async function connect(pi: PiExtensionApi, ctx: PiContext): Promise<void> {
  const session = await currentSession(ctx);
  const start = await api<{ device_code: string; user_code: string; verification_url: string; interval: number }>("/v1/connect/start", { method: "POST", body: JSON.stringify({ permissions: PERMISSIONS }) });
  notify(ctx, `Open ${start.verification_url} and approve agent code ${start.user_code}.`);
  const deadline = Date.now() + 10 * 60 * 1000;
  let approved: { status: string; address?: string; secret?: string } = { status: "pending" };
  while (Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, Math.max(1000, start.interval * 1000)));
    approved = await api<{ status: string; address?: string; secret?: string }>("/v1/connect/poll", { method: "POST", body: JSON.stringify({ device_code: start.device_code }) });
    if (approved.status === "approved") break;
  }
  if (approved.status !== "approved" || !approved.secret || !approved.address) throw new Error("Pi authorization expired before approval.");
  const state: BindingState = { address: approved.address, token: approved.secret, ...session, last_sequence: 0, pending_sequence: null, generation: 0, session_id: `pi-${crypto.randomUUID()}` };
  await writeState(state);
  await api("/v1/whoami", {}, state.token);
  await api<{ nonce: string; target_generation: number }>("/v1/provider/bind/start", { method: "POST", body: JSON.stringify({ provider: "pi" }) }, state.token);
  const binding = await api<{ binding: { target_generation: number } }>("/v1/provider/bind/pi", { method: "POST", body: JSON.stringify({ session_id: state.session_id }) }, state.token);
  state.generation = binding.binding.target_generation;
  await writeState(state);
  activeCtx = ctx;
  await openSocket(pi, ctx, state);
  notify(ctx, `myagent connected as ${state.address} in this Pi session.`);
  await drain(pi, ctx, state);
}

function scheduleReconnect(pi: PiExtensionApi, ctx: PiContext, state: BindingState): void {
  if (reconnectTimer) return;
  // Capped exponential backoff from 1 to 30 seconds.
  const delay = Math.min(30000, 1000 * 2 ** Math.min(reconnectAttempts, 5));
  reconnectAttempts += 1;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    void (async () => {
      // Reconnect only while the exact saved session is still bound here.
      const current = await readState();
      if (!current || current.session_id !== state.session_id || current.session_file !== state.session_file || current.cwd !== state.cwd) return;
      try {
        const status = await api<{ binding: { provider: string; status: string; external_target_id: string | null; target_generation: number } | null }>("/v1/provider/status", {}, current.token);
        if (!status.binding || status.binding.provider !== "pi" || status.binding.status !== "active" || status.binding.external_target_id !== current.session_id || status.binding.target_generation !== current.generation) return;
        await openSocket(pi, ctx, current);
        // Drain from the persisted bookmark; nothing was advanced while away.
        const fresh = (await readState()) ?? current;
        await drain(pi, ctx, fresh);
      } catch {
        scheduleReconnect(pi, ctx, state);
      }
    })();
  }, delay);
}

async function openSocket(pi: PiExtensionApi, ctx: PiContext, state: BindingState): Promise<void> {
  closeSocket();
  const websocketUrl = `${API_URL.replace(/^http/, "ws")}/v1/reach/live`;
  socket = new WebSocket(websocketUrl, { headers: { authorization: `Bearer ${state.token}` } });
  socket.on("message", () => { void drain(pi, ctx, state); });
  socket.on("close", () => {
    socket = null;
    scheduleReconnect(pi, ctx, state);
  });
  socket.on("error", () => { try { socket?.close(); } catch { /* noop */ } });
}

function streaming(ctx: PiContext): boolean {
  return typeof ctx.isStreaming === "function" ? ctx.isStreaming() : Boolean(ctx.isStreaming);
}

function envelopeText(entry: InboxEntry, replayed: boolean): string {
  const payload = entry.payload || {};
  const content = typeof payload.content === "object" && payload.content ? payload.content as Record<string, unknown> : null;
  const body = content?.encoding === "plaintext" && typeof content.text === "string"
    ? content.text
    : JSON.stringify(content ?? payload);
  const attachmentIds = Array.isArray(payload.attachment_ids) ? (payload.attachment_ids as unknown[]).map(String).join(", ") : "";
  return [
    "[myagent inbox — external_untrusted: true]",
    `message_id: ${String(payload.message_id || entry.event_id)}`,
    `destination_sequence: ${entry.sequence}`,
    `from_address: ${String(payload.from || "unknown")}`,
    `received_at: ${entry.occurred_at}`,
    "reply: use myagent_send_message with an explicit destination address",
    ...(replayed ? ["replayed: true (this entry was injected before; it was never bookmarked)"] : []),
    ...(attachmentIds ? [`attachments: ${attachmentIds}`] : []),
    "body:",
    body
  ].join("\n");
}

async function readOneEntry(state: BindingState, after: number): Promise<InboxEntry | null> {
  const result = await api<{ entries: InboxEntry[]; next_after_sequence: number | null }>(`/v1/inbox?after_sequence=${after}&limit=1`, {}, state.token);
  return result.entries[0] ?? null;
}

/** Drain exactly one inbox entry: persist it as pending, inject it as a user
 * turn, and stop. The bookmark advances only in onSettled, after Pi's
 * agent_settled event for that turn. Never more than one injected entry
 * awaits settlement. */
async function drain(pi: PiExtensionApi, ctx: PiContext, state: BindingState, replayPending = false): Promise<void> {
  if (draining) return;
  draining = true;
  try {
    // Do not queue an inbox entry behind an unrelated active turn. Otherwise
    // that turn's agent_settled event could bookmark the queued entry before
    // Pi has actually processed it. onSettled drains again once Pi is idle.
    if (streaming(ctx)) return;
    const saved = (await readState()) ?? state;
    // Resume the persisted view; a socket drop never advances the bookmark.
    state.last_sequence = saved.last_sequence;
    state.pending_sequence = saved.pending_sequence;
    if (state.pending_sequence !== null) {
      if (!replayPending) return;
      const pending = await readOneEntry(state, state.last_sequence);
      if (!pending || pending.sequence !== state.pending_sequence) {
        notify(ctx, "myagent could not replay the pending inbox entry; reconnect this session.");
        return;
      }
      pi.sendUserMessage(envelopeText(pending, true));
      return;
    }
    const entry = await readOneEntry(state, state.last_sequence);
    if (!entry || entry.sequence <= state.last_sequence) return;
    state.pending_sequence = entry.sequence;
    await writeState(state);
    try {
      pi.sendUserMessage(envelopeText(entry, false));
    } catch {
      // Injection failed: do not bookmark. The persisted pending marker is
      // replayed, with the same stable id and sequence, on session restart.
      notify(ctx, "myagent could not inject this inbox entry. Restart this Pi session to retry it.");
      return;
    }
    // No bookmark here — settlement decides.
  } finally {
    draining = false;
  }
}

/** Pi settled an agent turn. If a myagent entry is awaiting settlement,
 * bookmark it now, persist, clear pending, then drain the next entry. */
async function onSettled(pi: PiExtensionApi, ctx: PiContext): Promise<void> {
  const state = await readState();
  if (!state) return;
  if (state.pending_sequence === null) {
    await drain(pi, ctx, state);
    return;
  }
  const settled = state.pending_sequence;
  try {
    await api("/v1/inbox/bookmark", { method: "PUT", body: JSON.stringify({ sequence: settled }) }, state.token);
  } catch {
    // Bookmark failed: keep pending so the entry replays instead of being lost.
    return;
  }
  state.last_sequence = Math.max(state.last_sequence, settled);
  state.pending_sequence = null;
  await writeState(state);
  await drain(pi, ctx, state);
}

export default function registerMyagent(pi: PiExtensionApi): void {
  pi.registerTool({
    name: "myagent_send_message",
    label: "Send myagent message",
    description: "Send one plaintext message to an accepted myagent contact from the address connected to this exact Pi session.",
    parameters: Type.Object({
      to: Type.String({ description: "Destination myagent address, for example ajay/researcher." }),
      body: Type.String({ minLength: 1, maxLength: 32768, description: "Plaintext message body." }),
      message_id: Type.Optional(Type.String({ description: "Optional UUIDv7 idempotency key. Omit for a new message." }))
    }, { additionalProperties: false }),
    promptSnippet: "Send a message through the myagent address connected to this Pi session.",
    promptGuidelines: [
      "Use myagent_send_message to reply to a [myagent inbox] turn; never encode the destination in a TO: prefix.",
      "Only report delivery after the tool returns accepted."
    ],
    execute: async (_toolCallId, params, _signal, _onUpdate, ctx) => {
      const state = await requireActiveBinding(ctx);
      const messageId = params.message_id || uuidv7();
      const result = await api<{ accepted: true; duplicate: boolean; recipient_sequence: number; retention_expired?: true }>("/v1/messages", {
        method: "POST",
        body: JSON.stringify({
          message_id: messageId,
          to: params.to,
          content: { encoding: "plaintext", text: params.body },
          attachment_ids: []
        })
      }, state.token);
      return {
        content: [{ type: "text", text: `Message accepted for ${params.to} at recipient sequence ${result.recipient_sequence}${result.duplicate ? " (duplicate)" : ""}.` }],
        details: { message_id: messageId, to: params.to, ...result }
      };
    }
  });

  pi.registerCommand("myagent", {
    description: "Connect, inspect, or disconnect this exact Pi session from myagent",
    handler: async (args, ctx) => {
      const command = args.trim().split(/\s+/)[0] || "status";
      if (command === "connect") return connect(pi, ctx);
      if (command === "disconnect") return disconnect(ctx);
      if (command !== "status") throw new Error("Use /myagent connect, /myagent status, or /myagent disconnect.");
      const state = await readState();
      if (!state) return notify(ctx, "myagent is not connected in this Pi session.");
      const status = await api<{ binding: { provider: string; status: string; external_target_id: string | null; last_wake_sequence: number } }>("/v1/provider/status", {}, state.token);
      notify(ctx, `${status.binding?.provider || "myagent"}: ${status.binding?.status || "unknown"}; last wake ${status.binding?.last_wake_sequence ?? 0}.`);
    }
  });

  pi.on("session_start", async (_event, ctx) => {
    const state = await readState();
    if (!state) return;
    // Activate only for the exact bound session: both cwd and session file
    // must match. Another Pi session or workspace never consumes this inbox.
    const current = await currentSession(ctx);
    if (current.cwd !== state.cwd || current.session_file !== state.session_file) return;
    activeCtx = ctx;
    stopReconnect();
    await openSocket(pi, ctx, state);
    await drain(pi, ctx, state, true);
  });

  pi.on("agent_settled", async (_event, ctx) => {
    await onSettled(pi, ctx ?? activeCtx ?? ({} as PiContext));
  });

  pi.on("session_shutdown", async () => {
    stopReconnect();
    closeSocket();
  });
}
