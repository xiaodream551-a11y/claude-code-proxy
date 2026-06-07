import { describe, expect, it } from "bun:test";
import { CURSOR_AGENT_MODEL_IDS } from "./catalog.ts";
import { CURSOR_SUPPORTED_MODELS, isCursorModel, resolveCursorModel } from "./model.ts";

describe("Cursor model selection", () => {
  it("maps cursor aliases to composer fast", () => {
    const selected = resolveCursorModel({ model: "cursor", metadata: undefined });

    expect(selected.mode).toBe("AGENT_MODE_AGENT");
    expect(selected.requestedModel).toEqual({
      modelId: "composer-2.5",
      parameters: [{ id: "fast", value: "true" }],
    });
  });

  it("selects plan and ask modes from aliases", () => {
    expect(resolveCursorModel({ model: "cursor-plan", metadata: undefined }).mode).toBe(
      "AGENT_MODE_PLAN",
    );
    expect(resolveCursorModel({ model: "cursor-ask", metadata: undefined }).mode).toBe(
      "AGENT_MODE_ASK",
    );
  });

  it("supports raw cursor model prefix", () => {
    const selected = resolveCursorModel({
      model: "cursor:claude-sonnet-4-6-fast",
      metadata: { cursor_mode: "plan" },
    });

    expect(selected.mode).toBe("AGENT_MODE_PLAN");
    expect(selected.requestedModel).toEqual({
      modelId: "claude-sonnet-4-6",
      parameters: [{ id: "fast", value: "true" }],
    });
  });

  it("preserves exact Cursor catalog ids", () => {
    const selected = resolveCursorModel({
      model: "cursor:gpt-5.5-high-fast",
      metadata: undefined,
    });

    expect(selected.mode).toBe("AGENT_MODE_AGENT");
    expect(selected.requestedModel).toEqual({ modelId: "gpt-5.5-high-fast" });
  });

  it("supports prefixed plan and ask variants for catalog ids", () => {
    expect(resolveCursorModel({ model: "cursor-plan:gpt-5.5-high", metadata: undefined })).toEqual({
      mode: "AGENT_MODE_PLAN",
      requestedModel: { modelId: "gpt-5.5-high" },
    });
    expect(resolveCursorModel({ model: "cursor-ask:claude-opus-4-8-thinking-high", metadata: undefined })).toEqual({
      mode: "AGENT_MODE_ASK",
      requestedModel: { modelId: "claude-opus-4-8-thinking-high" },
    });
  });

  it("advertises current Cursor Agent models with unambiguous prefixes", () => {
    expect(CURSOR_AGENT_MODEL_IDS.length).toBeGreaterThan(100);
    expect(CURSOR_SUPPORTED_MODELS.has("cursor:gpt-5.5-high")).toBe(true);
    expect(CURSOR_SUPPORTED_MODELS.has("cursor-plan:gpt-5.5-high")).toBe(true);
    expect(CURSOR_SUPPORTED_MODELS.has("cursor-ask:gpt-5.5-high")).toBe(true);
    expect(isCursorModel("cursor:kimi-k2.5")).toBe(true);
    expect(isCursorModel("gpt-5.2")).toBe(false);
  });
});
