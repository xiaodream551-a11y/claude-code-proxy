import WebSocket from "ws";
import { headersToRecord } from "../../traffic.ts";
import type { RequestContext } from "../types.ts";
import type { ResponsesWebSocketRequest } from "./translate/request.ts";

export const WEBSOCKET_PROTOCOL_HEADER = "responses_websockets=2026-02-06";

export class CodexWebSocketSetupError extends Error {
  constructor(
    message: string,
    public status?: number,
    public code?: string,
    public retryAfter?: string,
  ) {
    super(message);
    this.name = "CodexWebSocketSetupError";
  }
}

export interface CodexWebSocketOptions {
  url: string;
  headers: Headers;
  body: ResponsesWebSocketRequest;
  ctx: RequestContext;
  connectTimeoutMs: number;
  idleTimeoutMs: number;
}

export function toWebSocketUrl(url: string): string {
  const parsed = new URL(url);
  if (parsed.protocol === "http:") {
    parsed.protocol = "ws:";
    return parsed.toString();
  }
  if (parsed.protocol === "https:") {
    parsed.protocol = "wss:";
    return parsed.toString();
  }
  throw new Error(`Unsupported Codex WebSocket URL scheme: ${parsed.protocol}`);
}

export function codexWebSocketHeaders(headers: Headers): Record<string, string> {
  const requestHeaders = headersToRecord(headers);
  requestHeaders["openai-beta"] = WEBSOCKET_PROTOCOL_HEADER;
  delete requestHeaders["content-length"];
  return requestHeaders;
}

export async function codexWebSocketRequest(
  opts: CodexWebSocketOptions,
): Promise<ReadableStream<Uint8Array>> {
  const encoder = new TextEncoder();
  const wsUrl = toWebSocketUrl(opts.url);
  const requestHeaders = codexWebSocketHeaders(opts.headers);

  opts.ctx.traffic?.writeJson("022-upstream-websocket-metadata", {
    provider: "codex",
    url: wsUrl,
    headers: requestHeaders,
  });

  return new Promise((resolve, reject) => {
    const socket = new WebSocket(wsUrl, { headers: requestHeaders });
    let controller: ReadableStreamDefaultController<Uint8Array> | undefined;
    let settled = false;
    let done = false;
    let idleTimer: ReturnType<typeof setTimeout> | undefined;
    let connectTimer: ReturnType<typeof setTimeout> | undefined;
    const pendingChunks: Uint8Array[] = [];

    const stream = new ReadableStream<Uint8Array>({
      start(next) {
        controller = next;
        for (const chunk of pendingChunks.splice(0)) controller.enqueue(chunk);
      },
      cancel() {
        done = true;
        cleanup();
        socket.on("error", () => {});
        socket.terminate();
      },
    });

    const cleanup = () => {
      if (idleTimer) clearTimeout(idleTimer);
      if (connectTimer) clearTimeout(connectTimer);
      opts.ctx.signal.removeEventListener("abort", onAbort);
    };
    const fail = (err: Error) => {
      if (done) return;
      done = true;
      cleanup();
      socket.on("error", () => {});
      socket.terminate();
      if (!settled) reject(err);
      else controller?.error(err);
    };
    const enqueue = (chunk: Uint8Array) => {
      if (controller) controller.enqueue(chunk);
      else pendingChunks.push(chunk);
    };
    const closeController = () => {
      if (controller) controller.close();
      else queueMicrotask(closeController);
    };
    const close = () => {
      if (done) return;
      done = true;
      cleanup();
      closeController();
      socket.close();
    };
    const resetIdle = () => {
      if (idleTimer) clearTimeout(idleTimer);
      idleTimer = setTimeout(
        () => fail(new Error("Timed out waiting for Codex WebSocket event")),
        opts.idleTimeoutMs,
      );
    };
    const onAbort = () => fail(new DOMException("Aborted", "AbortError"));
    const encodeSse = (text: string) =>
      `${text
        .split(/\r?\n/)
        .map((line) => `data: ${line}`)
        .join("\n")}\n\n`;

    connectTimer = setTimeout(
      () => fail(new Error("Timed out connecting to Codex WebSocket")),
      opts.connectTimeoutMs,
    );
    opts.ctx.signal.addEventListener("abort", onAbort, { once: true });

    socket.on("unexpected-response", (_req, res) => {
      fail(
        new CodexWebSocketSetupError(
          `Unexpected Codex WebSocket upgrade response: ${res.statusCode}`,
          res.statusCode,
        ),
      );
    });
    socket.on("open", () => {
      if (connectTimer) clearTimeout(connectTimer);
      resetIdle();
      const { stream: _stream, ...payload } = opts.body;
      try {
        socket.send(JSON.stringify({ type: "response.create", ...payload }), (err) => {
          if (err) fail(err instanceof Error ? err : new Error(String(err)));
        });
      } catch (err) {
        fail(err instanceof Error ? err : new Error(String(err)));
      }
    });
    socket.on("message", (data, isBinary) => {
      if (isBinary) {
        fail(new Error("Unexpected binary Codex WebSocket frame"));
        return;
      }
      resetIdle();
      const text = data.toString();
      let event: {
        type?: string;
        status?: number;
        error?: { code?: string; message?: string };
        headers?: Record<string, string>;
      };
      try {
        event = JSON.parse(text);
      } catch (err) {
        fail(err instanceof Error ? err : new Error(String(err)));
        return;
      }
      if (!settled && event.type === "error") {
        fail(
          new CodexWebSocketSetupError(
            event.error?.message ?? event.error?.code ?? "Codex WebSocket setup error",
            event.status,
            event.error?.code,
            event.headers?.["retry-after"],
          ),
        );
        return;
      }
      enqueue(encoder.encode(encodeSse(text)));
      if (!settled) {
        settled = true;
        resolve(stream);
      }
      if (
        event.type === "response.completed" ||
        event.type === "response.failed" ||
        event.type === "response.incomplete" ||
        event.type === "response.done" ||
        event.type === "error"
      ) {
        close();
      }
    });
    socket.on("error", (err) => fail(err instanceof Error ? err : new Error(String(err))));
    socket.on("close", (code, reason) => {
      if (!done) fail(new Error(`Codex WebSocket closed before terminal event: ${code} ${reason}`));
    });
  });
}
