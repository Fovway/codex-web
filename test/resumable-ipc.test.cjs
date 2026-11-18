const assert = require("node:assert/strict");
const { once } = require("node:events");
const { randomBytes } = require("node:crypto");
const test = require("node:test");
const { WebSocket, WebSocketServer } = require("ws");
const { createResumableIpc } = require("../src/server/resumable-ipc.js");

test(
  "replay server events and deduplicate client events after reconnect",
  { timeout: 5000 },
  async (t) => {
    const server = new WebSocketServer({ host: "127.0.0.1", port: 0 });
    t.after(() => {
      for (const socket of server.clients) socket.terminate();
      server.close();
    });

    const renderers = [];
    server.on(
      "connection",
      createResumableIpc((send) => {
        const renderer = { send, received: [] };
        renderers.push(renderer);
        return {
          receive(message) {
            renderer.received.push(message);
          },
          close() {},
        };
      }),
    );
    await once(server, "listening");
    const endpoint = `ws://127.0.0.1:${server.address().port}`;
    const token = randomBytes(32).toString("hex");

    function connect(resume, ack) {
      const socket = new WebSocket(endpoint);
      const frames = [];
      const pending = [];
      const send = (frame) => socket.send(JSON.stringify(frame));
      socket.on("open", () => send({ type: "hello", token, resume, ack }));
      socket.on("message", (data) => {
        const frame = JSON.parse(data);
        if (pending.length) pending.shift()(frame);
        else frames.push(frame);
      });
      return {
        socket,
        send,
        next: () =>
          frames.length
            ? Promise.resolve(frames.shift())
            : new Promise((resolve) => pending.push(resolve)),
      };
    }

    const first = connect(false, 0);
    assert.deepEqual(await first.next(), { type: "ready", ack: 0 });
    const request = {
      type: "event",
      sequence: 1,
      message: { operation: "start" },
    };
    first.send(request);
    assert.deepEqual(await first.next(), { type: "ack", sequence: 1 });
    renderers[0].send({ status: "in progress" });
    assert.equal((await first.next()).sequence, 1);
    const closed = once(first.socket, "close");
    first.socket.close();
    await closed;
    renderers[0].send({ status: "finished" });

    const resumed = connect(true, 1);
    assert.deepEqual(await resumed.next(), { type: "ready", ack: 1 });
    assert.deepEqual(await resumed.next(), {
      type: "event",
      sequence: 2,
      message: { status: "finished" },
    });
    resumed.send(request);
    assert.deepEqual(await resumed.next(), { type: "ack", sequence: 1 });
    assert.equal(renderers.length, 1);
    assert.deepEqual(renderers[0].received, [request.message]);
  },
);
