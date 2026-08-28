// Emits the AWS-native generated code for the hotel occupancy-forecast model
// into this CDK app. Regenerate with:  node emit.mjs
//
// It calls the mermaid-event-model code generator (the parent repo, ..)
// and writes:
//   src/shared/event-store.ts        — shared runtime (part: 'runtime')
//   infra/stacks/regional-stack.ts   — the regional CDK stack (part: 'infra')
//   src/commands/handler.ts          — consolidated command handler (all commands)
//   src/projector/handler.ts         — DynamoDB Streams → Redis projector
//   src/queries/handler.ts           — API Gateway read handler
//   slices/<slice>.ts                — one file per slice (reference; the
//                                      generator's native per-slice output)
//
// The three fixed entry paths the stack references (commands/queries/projector)
// are the consolidated model-level handlers; slices/ holds the per-slice output
// for reference and audit.
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
// The generator lives one level up: this app is a subdirectory of the
// mermaid-event-model repo (aws-app/).
const LIB = path.resolve(here, "..");
const MODEL_FILE = path.join(LIB, "blueprint_dsl_dcb.md");
const SLICES_DIR = path.join(LIB, "blueprint_dsl_dcb-slices");

const { generateAwsFromSource } = await import(path.join(LIB, "codegen.js"));
const { parseEventModel } = await import(path.join(LIB, "event-model.js"));

const write = (rel, contents) => {
  const dest = path.join(here, rel);
  fs.mkdirSync(path.dirname(dest), { recursive: true });
  fs.writeFileSync(dest, contents);
  console.log("wrote", rel, `(${contents.length} bytes)`);
};

const modelSrc = fs.readFileSync(MODEL_FILE, "utf8");
const model = parseEventModel(modelSrc);

// 1) Shared runtime (event-store envelope, EventTypes, DCB primitives,
//    Kinesis + Redis + SageMaker helpers).
write("src/shared/event-store.ts", generateAwsFromSource(modelSrc, { part: "runtime" }));

// 2) Regional CDK stack (VPC, DynamoDB ref, Kinesis, Redis, Lambdas, API GW,
//    SageMaker grant). Minimal tier to keep the synth cheap/simple.
write(
  "infra/stacks/regional-stack.ts",
  generateAwsFromSource(modelSrc, { part: "infra", tier: "minimal" })
);

// 3) Consolidated command handler: the whole model as a single slice emits one
//    handler() that routes every command. This is the write side the stack's
//    src/commands/handler.ts entry points at.
write("src/commands/handler.ts", generateAwsFromSource(modelSrc, { part: "slice", sliceName: "commands" }));

// 4) Projector + query: generated from the whole model's view surface. A
//    view-only source emits handler() (projector) + queryHandler() (query).
//    We pick a representative view slice that folds the widest event set, then
//    write thin entry files that re-export the expected symbol name.
const viewSliceFile = "view-room-availability.md";
const viewSrc = fs.readFileSync(path.join(SLICES_DIR, viewSliceFile), "utf8");
write("src/_generated/view.ts", generateAwsFromSource(viewSrc, { sliceName: "views" }));
write(
  "src/projector/handler.ts",
  `// Projector entry — re-exports the generated projector (DynamoDB Streams → Redis).\nexport { handler } from '../_generated/view';\n`
);
write(
  "src/queries/handler.ts",
  `// Query entry — re-exports the generated query handler (API Gateway → Redis).\nexport { queryHandler as handler } from '../_generated/view';\n`
);

// 5) Per-slice reference output (the generator's native artifact per slice).
const elById = new Map(model.elements.map((e) => [e.id, e]));
for (const s of model.slices) {
  // Find the matching slice spec file by slug.
  const slug = s.id.replace(/_/g, "-");
  const candidates = fs.readdirSync(SLICES_DIR).filter((f) => f.replace(/\.md$/, "") === slug);
  const src = candidates.length
    ? fs.readFileSync(path.join(SLICES_DIR, candidates[0]), "utf8")
    : modelSrc;
  try {
    const ts = generateAwsFromSource(src, { sliceName: s.id });
    write(`slices/${s.id}.ts`, ts);
  } catch (err) {
    console.warn(`  skip slice ${s.id}: ${err.message}`);
  }
}

console.log("\nemit complete.");
