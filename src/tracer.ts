import * as fs from "node:fs";
import * as core from "@actions/core";
import * as grpc from "@grpc/grpc-js";
import { context } from "@opentelemetry/api";
import type { Attributes } from "@opentelemetry/api";
import { AsyncHooksContextManager } from "@opentelemetry/context-async-hooks";
import { ExportResultCode } from "@opentelemetry/core";
import { OTLPTraceExporter as GrpcOTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-grpc";
import { OTLPTraceExporter as ProtoOTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-proto";
import { Resource, type ResourceAttributes } from "@opentelemetry/resources";

import {
  BasicTracerProvider,
  BatchSpanProcessor,
  type IdGenerator,
  type ReadableSpan,
  type SpanExporter,
} from "@opentelemetry/sdk-trace-base";

function writeObjectToJsonFile<T>(filePath: string, obj: T): Promise<string> {
  return new Promise((resolve, reject) => {
    fs.writeFile(filePath, JSON.stringify(obj, null, 2), "utf8", (err) => {
      if (err) {
        reject(err);
      } else {
        resolve(`Object written to ${filePath}`);
      }
    });
  });
}

interface ExportedSpanData {
  traceId: string;
  spanId: string;
  parentSpanId: string | null;
  name: string;
  kind: number;
  startTime: [number, number];
  endTime: [number, number];
  attributes: Record<string, unknown>;
  events: {
    name: string;
    time: [number, number];
    attributes?: Attributes | undefined;
  }[];
  status: {
    code: number;
    message?: string;
  };
}

let spansStorage: ExportedSpanData[] = []; // In-memory storage for spans

class JSONSpanExporter implements SpanExporter {
  export(spans: ReadableSpan[], resultCallback: (result: { code: ExportResultCode }) => void): void {
    core.info(`Starting Spans: ${spans}`);
    for (const span of spans) {
      spansStorage.push({
        traceId: span.spanContext().traceId,
        spanId: span.spanContext().spanId,
        parentSpanId: span.parentSpanId || null,
        name: span.name,
        kind: span.kind,
        startTime: span.startTime,
        endTime: span.endTime,
        attributes: span.attributes,
        status: span.status,
        events: span.events.map((event) => ({
          name: event.name,
          time: event.time,
          attributes: event.attributes,
        })),
      });
    }

    core.info(`Resulting Spans: ${spansStorage}`);

    // Indicate successful export
    resultCallback({ code: ExportResultCode.SUCCESS });
  }
  async forceFlush(): Promise<void> {
    const filename = "data.json";
    core.info(`Starting Spans to be Written: ${spansStorage}`);

    writeObjectToJsonFile(filename, spansStorage);

    core.info(`File Written Successfully to ${filename}`);
    return Promise.resolve();
  }

  // Graceful shutdown
  async shutdown(): Promise<void> {
    core.info("Calling from inner Shutdown()");
    spansStorage = []; // Optionally clear storage on shutdown
    return Promise.resolve();
  }
}

const OTEL_CONSOLE_ONLY = process.env["OTEL_CONSOLE_ONLY"] === "true";
const OTEL_ID_SEED = Number.parseInt(process.env["OTEL_ID_SEED"] ?? "0");

function stringToRecord(s: string) {
  const record: Record<string, string> = {};

  for (const pair of s.split(",")) {
    const [key, value] = pair.split(/=(.*)/s);
    if (key && value) {
      record[key.trim()] = value.trim();
    }
  }
  return record;
}

function isHttpEndpoint(endpoint: string) {
  return endpoint.startsWith("https://") || endpoint.startsWith("http://");
}

function createTracerProvider(endpoint: string, headers: string, attributes: ResourceAttributes) {
  // Register the context manager to enable context propagation
  const contextManager = new AsyncHooksContextManager();
  contextManager.enable();
  context.setGlobalContextManager(contextManager);

  let exporter: SpanExporter = new JSONSpanExporter(); // Use custom exporter

  // let exporter: SpanExporter = new ConsoleSpanExporter();

  if (!OTEL_CONSOLE_ONLY) {
    if (isHttpEndpoint(endpoint)) {
      exporter = new ProtoOTLPTraceExporter({
        url: endpoint,
        headers: stringToRecord(headers),
      });
    } else {
      exporter = new GrpcOTLPTraceExporter({
        url: endpoint,
        credentials: grpc.credentials.createSsl(),
        metadata: grpc.Metadata.fromHttp2Headers(stringToRecord(headers)),
      });
    }
  }

  const provider = new BasicTracerProvider({
    resource: new Resource(attributes),
    spanProcessors: [new BatchSpanProcessor(exporter)],
    ...(OTEL_ID_SEED && { idGenerator: new DeterministicIdGenerator(OTEL_ID_SEED) }),
  });

  provider.register();
  return provider;
}

// Copied from xorshift32amx here: https://github.com/bryc/code/blob/master/jshash/PRNGs.md#xorshift
function createRandomWithSeed(seed: number) {
  let a = seed;
  return function getRandomInt(max: number) {
    let t = Math.imul(a, 1597334677);
    t = (t >>> 24) | ((t >>> 8) & 65280) | ((t << 8) & 16711680) | (t << 24); // reverse byte order
    a ^= a << 13;
    a ^= a >>> 17;
    a ^= a << 5;
    const res = ((a + t) >>> 0) / 4294967296;

    return Math.floor(res * max);
  };
}

/**
 * A deterministic id generator for testing purposes.
 */
class DeterministicIdGenerator implements IdGenerator {
  readonly characters = "0123456789abcdef";
  getRandomInt: (max: number) => number;

  constructor(seed: number) {
    this.getRandomInt = createRandomWithSeed(seed);
  }

  generateTraceId() {
    return this.generateId(32);
  }

  generateSpanId() {
    return this.generateId(16);
  }

  private generateId(length: number) {
    let id = "";

    for (let i = 0; i < length; i++) {
      id += this.characters[this.getRandomInt(this.characters.length)];
    }
    return id;
  }
}
export { stringToRecord, createTracerProvider };
