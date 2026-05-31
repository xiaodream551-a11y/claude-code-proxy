import { afterEach, describe, expect, it } from "bun:test";
import { createServer } from "node:http";
import WebSocket, { WebSocketServer } from "ws";
import type { RequestContext } from "../types.ts";
import {
  CodexWebSocketSetupError,
  clearCodexWebSocketPoolForTests,
  codexWebSocketHeaders,
  codexWebSocketRequest,
  toWebSocketUrl,
  WEBSOCKET_PROTOCOL_HEADER,
} from "./websocket.ts";
import type { ResponsesWebSocketRequest } from "./translate/request.ts";

const silentLog = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
  child: () => silentLog,
};

function ctx(): RequestContext {
  return {
    reqId: "req_1",
    signal: new AbortController().signal,
    childLogger: () => silentLog,
  };
}

afterEach(() => {
  clearCodexWebSocketPoolForTests();
});

function body(): ResponsesWebSocketRequest {
  return {
    model: "gpt-5.5",
    input: [{ type: "message", role: "user", content: [{ type: "input_text", text: "hello" }] }],
    store: false,
    stream: true,
  };
}

async function collect(stream: ReadableStream<Uint8Array>): Promise<string> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let out = "";
  while (true) {
    const { done, value } = await reader.read();
    if (done) return out;
    out += decoder.decode(value, { stream: true });
  }
}

async function withServer(
  handler: (socket: WebSocket, requestBody: Promise<unknown>, request: import("node:http").IncomingMessage) => void,
): Promise<{ url: string; close: () => Promise<void> }> {
  const server = createServer();
  const wss = new WebSocketServer({ server });
  wss.on("connection", (socket, request) => {
    const messages: unknown[] = [];
    const waiters: ((value: unknown) => void)[] = [];
    const nextBody = () =>
      new Promise<unknown>((resolve) => {
        const message = messages.shift();
        if (message !== undefined) resolve(message);
        else waiters.push(resolve);
      });
    socket.on("message", (data) => {
      const message = JSON.parse(data.toString());
      const waiter = waiters.shift();
      if (waiter) waiter(message);
      else messages.push(message);
    });
    handler(socket, nextBody(), request);
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("missing server address");
  return {
    url: `http://127.0.0.1:${address.port}/backend-api/codex/responses`,
    close: () =>
      new Promise<void>((resolve, reject) => {
        wss.close((err) => {
          if (err) reject(err);
          else server.close((closeErr) => (closeErr ? reject(closeErr) : resolve()));
        });
      }),
  };
}

describe("Codex WebSocket helpers", () => {
  it("converts HTTP URLs to WebSocket URLs", () => {
    expect(toWebSocketUrl("https://chatgpt.com/backend-api/codex/responses")).toBe(
      "wss://chatgpt.com/backend-api/codex/responses",
    );
    expect(toWebSocketUrl("http://127.0.0.1:1234/backend-api/codex/responses")).toBe(
      "ws://127.0.0.1:1234/backend-api/codex/responses",
    );
  });

  it("rejects unsupported URL schemes", () => {
    expect(() => toWebSocketUrl("file:///tmp/responses")).toThrow(
      "Unsupported Codex WebSocket URL scheme",
    );
  });

  it("sets the WebSocket beta header and removes content length", () => {
    const headers = new Headers({
      "openai-beta": "responses=experimental",
      "content-length": "123",
      authorization: "Bearer token",
    });

    expect(codexWebSocketHeaders(headers)).toEqual({
      authorization: "Bearer token",
      "openai-beta": WEBSOCKET_PROTOCOL_HEADER,
    });
  });

  it("carries upgrade failure status for auth refresh", () => {
    const err = new CodexWebSocketSetupError("upgrade failed", 401);

    expect(err.name).toBe("CodexWebSocketSetupError");
    expect(err.status).toBe(401);
  });

  it("converts websocket events to SSE and sends response.create without stream", async () => {
    const server = await withServer(async (socket, requestBody) => {
      const req = await requestBody;
      expect(req).toEqual({
        type: "response.create",
        model: "gpt-5.5",
        input: [
          { type: "message", role: "user", content: [{ type: "input_text", text: "hello" }] },
        ],
        store: false,
      });
      socket.send(JSON.stringify({ type: "response.completed", response: { id: "resp_1" } }));
    });
    try {
      const stream = await codexWebSocketRequest({
        url: server.url,
        headers: new Headers(),
        body: body(),
        ctx: ctx(),
        connectTimeoutMs: 1_000,
        idleTimeoutMs: 1_000,
      });

      await expect(collect(stream)).resolves.toBe(
        'data: {"type":"response.completed","response":{"id":"resp_1"}}\n\n',
      );
    } finally {
      await server.close();
    }
  });

  it("rejects setup errors before exposing a stream", async () => {
    const server = await withServer((socket) => {
      socket.send(
        JSON.stringify({
          type: "error",
          status: 429,
          error: { code: "rate_limit", message: "slow down" },
          headers: { "retry-after": "3" },
        }),
      );
    });
    try {
      let caught: unknown;
      try {
        await codexWebSocketRequest({
          url: server.url,
          headers: new Headers(),
          body: body(),
          ctx: ctx(),
          connectTimeoutMs: 1_000,
          idleTimeoutMs: 1_000,
        });
      } catch (err) {
        caught = err;
      }
      expect(caught).toBeInstanceOf(CodexWebSocketSetupError);
      const err = caught as CodexWebSocketSetupError;
      expect(err.status).toBe(429);
      expect(err.code).toBe("rate_limit");
      expect(err.retryAfter).toBe("3");
      expect(err.requestSent).toBe(false);
    } finally {
      await server.close();
    }
  });

  it("exposes response errors as SSE events", async () => {
    const server = await withServer(async (socket, requestBody) => {
      await requestBody;
      socket.send(
        JSON.stringify({
          type: "error",
          error: { code: "invalid_request", message: "bad request" },
        }),
      );
    });
    try {
      const stream = await codexWebSocketRequest({
        url: server.url,
        headers: new Headers(),
        body: body(),
        ctx: ctx(),
        connectTimeoutMs: 1_000,
        idleTimeoutMs: 1_000,
      });

      await expect(collect(stream)).resolves.toBe(
        'data: {"type":"error","error":{"code":"invalid_request","message":"bad request"}}\n\n',
      );
    } finally {
      await server.close();
    }
  });

  it("reuses a pooled websocket", async () => {
    let sockets = 0;
    const server = await withServer((socket) => {
      sockets++;
      socket.on("message", () => {
        socket.send(JSON.stringify({ type: "response.completed", response: { id: `resp_${sockets}` } }));
      });
    });
    try {
      const request = () =>
        codexWebSocketRequest({
          url: server.url,
          headers: new Headers(),
          body: body(),
          ctx: ctx(),
          connectTimeoutMs: 1_000,
          idleTimeoutMs: 1_000,
          poolKey: "session-1",
        });

      await collect(await request());
      await collect(await request());

      expect(sockets).toBe(1);
    } finally {
      clearCodexWebSocketPoolForTests();
      await server.close();
    }
  });

  it("does not pool websocket requests without a pool key", async () => {
    let sockets = 0;
    const server = await withServer((socket) => {
      sockets++;
      socket.on("message", () => {
        socket.send(JSON.stringify({ type: "response.completed", response: { id: `resp_${sockets}` } }));
      });
    });
    try {
      const makeRequest = () =>
        codexWebSocketRequest({
          url: server.url,
          headers: new Headers(),
          body: body(),
          ctx: ctx(),
          connectTimeoutMs: 1_000,
          idleTimeoutMs: 1_000,
        }).then(collect);

      await makeRequest();
      await makeRequest();
      expect(sockets).toBe(2);
    } finally {
      await server.close();
    }
  });
});
