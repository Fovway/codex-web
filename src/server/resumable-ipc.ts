import { WebSocket } from "ws";

const SESSION_TTL_MS = 20 * 60 * 1000;
const HEARTBEAT_MS = 15_000;
const MAX_SESSIONS = 128;
const MAX_FRAMES = 4096;
const MAX_SESSION_BYTES = 8 * 1024 * 1024;
const MAX_TOTAL_BYTES = 48 * 1024 * 1024;
const TOKEN_PATTERN = /^[a-f0-9]{64}$/;

type Renderer<Inbound> = {
  receive(message: Inbound): void;
  close(): void;
};

type Session<Inbound> = {
  token: string;
  socket: WebSocket | null;
  expiry: NodeJS.Timeout | null;
  serverSequence: number;
  clientSequence: number;
  acknowledged: number;
  bytes: number;
  frames: Map<number, { text: string; bytes: number }>;
  renderer: Renderer<Inbound>;
};

export function createResumableIpc<Inbound, Outbound>(
  createRenderer: (send: (message: Outbound) => void) => Renderer<Inbound>,
): (socket: WebSocket) => void {
  const sessions = new Map<string, Session<Inbound>>();
  let totalBytes = 0;

  function discardThrough(session: Session<Inbound>, sequence: number) {
    for (const [id, frame] of session.frames) {
      if (id > sequence) break;
      session.frames.delete(id);
      session.bytes -= frame.bytes;
      totalBytes -= frame.bytes;
    }
    session.acknowledged = sequence;
  }

  function expire(session: Session<Inbound>) {
    sessions.delete(session.token);
    if (session.expiry !== null) clearTimeout(session.expiry);
    discardThrough(session, session.serverSequence);
    session.socket?.close(4001, "Renderer session expired");
    session.socket = null;
    session.renderer.close();
  }

  function createSession(token: string): Session<Inbound> {
    const state = {
      token,
      socket: null,
      expiry: null,
      serverSequence: 0,
      clientSequence: 0,
      acknowledged: 0,
      bytes: 0,
      frames: new Map<number, { text: string; bytes: number }>(),
    };
    const renderer = createRenderer((message) => {
      if (sessions.get(token) !== session) return;
      const sequence = ++session.serverSequence;
      const text = JSON.stringify({ type: "event", sequence, message });
      const bytes = Buffer.byteLength(text);
      if (
        session.frames.size >= MAX_FRAMES ||
        session.bytes + bytes > MAX_SESSION_BYTES ||
        totalBytes + bytes > MAX_TOTAL_BYTES
      ) {
        expire(session);
        return;
      }
      session.frames.set(sequence, { text, bytes });
      session.bytes += bytes;
      totalBytes += bytes;
      if (session.socket?.readyState === WebSocket.OPEN)
        session.socket.send(text);
    });
    const session: Session<Inbound> = { ...state, renderer };
    sessions.set(token, session);
    return session;
  }

  return (socket) => {
    let session: Session<Inbound> | undefined;
    let alive = true;
    const handshake = setTimeout(
      () => socket.close(1008, "Expected IPC handshake"),
      10_000,
    );
    const heartbeat = setInterval(() => {
      if (socket.readyState !== WebSocket.OPEN) return;
      if (!alive) {
        socket.terminate();
        return;
      }
      alive = false;
      socket.ping();
    }, HEARTBEAT_MS);
    socket.on("pong", () => {
      alive = true;
    });
    socket.on("error", () => socket.terminate());

    function send(frame: object) {
      if (socket.readyState === WebSocket.OPEN)
        socket.send(JSON.stringify(frame));
    }

    function acknowledge(
      session: Session<Inbound>,
      sequence: unknown,
    ): boolean {
      if (
        typeof sequence !== "number" ||
        !Number.isSafeInteger(sequence) ||
        sequence < session.acknowledged ||
        sequence > session.serverSequence
      ) {
        socket.close(1008, "Invalid IPC acknowledgement");
        return false;
      }
      discardThrough(session, sequence);
      return true;
    }

    socket.on("message", (rawData) => {
      let frame: Record<string, unknown>;
      try {
        frame = JSON.parse(String(rawData)) as Record<string, unknown>;
      } catch {
        socket.close(1007, "Invalid IPC JSON");
        return;
      }
      if (frame === null || typeof frame !== "object") {
        socket.close(1008, "Invalid IPC frame");
        return;
      }

      if (!session) {
        if (
          frame.type !== "hello" ||
          typeof frame.token !== "string" ||
          !TOKEN_PATTERN.test(frame.token) ||
          typeof frame.resume !== "boolean"
        ) {
          socket.close(1008, "Invalid IPC handshake");
          return;
        }
        const existing = sessions.get(frame.token);
        if (!existing && frame.resume) {
          socket.close(4001, "Renderer session expired");
          return;
        }
        if (!existing && (frame.ack !== 0 || sessions.size >= MAX_SESSIONS)) {
          socket.close(1013, "Renderer unavailable");
          return;
        }
        session = existing ?? createSession(frame.token);
        if (!acknowledge(session, frame.ack)) return;

        clearTimeout(handshake);
        if (session.expiry !== null) clearTimeout(session.expiry);
        const previous = session.socket;
        session.socket = socket;
        previous?.terminate();
        send({ type: "ready", ack: session.clientSequence });
        for (const { text } of session.frames.values()) socket.send(text);
        return;
      }

      if (session.socket !== socket || socket.readyState !== WebSocket.OPEN)
        return;
      if (frame.type === "ack") {
        acknowledge(session, frame.sequence);
        return;
      }
      if (
        frame.type !== "event" ||
        typeof frame.sequence !== "number" ||
        !Number.isSafeInteger(frame.sequence) ||
        frame.sequence < 1 ||
        frame.sequence > session.clientSequence + 1 ||
        !frame.message ||
        typeof frame.message !== "object"
      ) {
        socket.close(1008, "Invalid IPC event");
        return;
      }
      if (frame.sequence > session.clientSequence) {
        session.clientSequence = frame.sequence;
        session.renderer.receive(frame.message as Inbound);
      }
      send({ type: "ack", sequence: session.clientSequence });
    });

    socket.on("close", () => {
      clearTimeout(handshake);
      clearInterval(heartbeat);
      if (!session || session.socket !== socket) return;
      session.socket = null;
      const disconnected = session;
      disconnected.expiry = setTimeout(
        () => expire(disconnected),
        SESSION_TTL_MS,
      );
      disconnected.expiry.unref();
    });
  };
}
