#!/usr/bin/env node
// Build-only plugin. Use Protobuf-ES's own symbol/import resolver so nested
// messages, well-known types, keywords and import paths match its message code.
import { createEcmaScriptPlugin, runNodeJs } from "@bufbuild/protoplugin";
import ts from "typescript-transpile";
import { resolve, relative } from "node:path";
import { fileURLToPath } from "node:url";

export const plugin = createEcmaScriptPlugin({
  name: "protoc-gen-cellar",
  version: "v0.1.0",

  // Service APIs use global Promise and private class fields. The framework's
  // default transpiler has no standard library; supply the pinned compiler's
  // real ES library for declarations. Full generated-code typechecking runs
  // separately with message files and the SDK installed.
  transpile(files, javascript, declarations, style) {
    const root = resolve(".cellar-codegen-virtual");
    const sources = new Map(files.map(file => [resolve(root, file.name), file]));
    const options = {
      target: ts.ScriptTarget.ES2022,
      module: style === "legacy_commonjs" ? ts.ModuleKind.CommonJS : ts.ModuleKind.ESNext,
      declaration: declarations,
      emitDeclarationOnly: declarations && !javascript,
      lib: ["lib.es2023.d.ts"],
      skipLibCheck: true,
      noEmitOnError: false,
    };
    const host = ts.createCompilerHost(options);
    const read = host.getSourceFile.bind(host);
    host.getSourceFile = (name, language, onError, fresh) => sources.has(name)
      ? ts.createSourceFile(name, sources.get(name).content, language)
      : read(name, language, onError, fresh);
    const program = ts.createProgram([...sources.keys()], options, host);
    const output = [];
    const emitted = program.emit(undefined, (name, content, _bom, _error, input) => {
      const original = input?.length === 1 && sources.get(input[0].fileName);
      if (!original) throw new Error("Unmapped generated service output");
      output.push({name:relative(root, name).replaceAll("\\", "/"), content, preamble:original.preamble});
    });
    if (emitted.emitSkipped) throw new Error("Cellar service declaration generation failed");
    return output;
  },

  parseOptions(options) {
    let digest;
    for (const option of options) {
      if (option.key !== "digest" || !/^[a-f0-9]{64}$/.test(option.value) || digest !== undefined) {
        throw new Error("Expected one exact contract digest");
      }
      digest = option.value;
    }
    if (digest === undefined) throw new Error("A contract digest is required");
    return { digest };
  },

  generateTs(schema) {
    for (const file of schema.files) {
      if (file.services.length === 0) continue;
      const out = schema.generateFile(`${file.name}_cellar.ts`);
      out.preamble(file);
      const runtime = name => out.import(name, "@cellar/sdk/rpc");
      const Context = out.import("Context", "@cellar/sdk", true);
      const Options = out.import("RpcCallOptions", "@cellar/sdk/rpc", true);
      const Registration = out.import("RpcRegistration", "@cellar/sdk/rpc", true);
      const Initializer = out.import("MessageInitShape", "@bufbuild/protobuf", true);
      const create = out.import("create", "@bufbuild/protobuf");
      const fromBinary = out.import("fromBinary", "@bufbuild/protobuf");
      const toBinary = out.import("toBinary", "@bufbuild/protobuf");
      const configure = out.import("configureTextEncoding", "@bufbuild/protobuf/wire");
      out.print("const codec = ", runtime("createCodec"), "({create:", create,
        ", fromBinary:", fromBinary, ", toBinary:", toBinary,
        ", configureTextEncoding:", configure, "});");
      out.print();

      for (const service of file.services) {
        // Proto identifiers are valid in these generated names. The shared
        // native generator also rejects language/helper identifier collisions.
        const name = service.name;
        const rules = [];
        for (const method of service.methods) {
          if (method.methodKind !== "unary") throw new Error("Cellar supports unary RPC methods");
          const rule = `_${name}_${method.localName}`;
          rules.push(rule);
          out.print("const ", rule, " = ", runtime("createMethod"), "({");
          out.print("  method:", out.string(`${service.typeName}/${method.name}`), ",");
          out.print("  digest:", out.string(schema.options.digest), ",");
          out.print("  handler:", out.string(method.localName), ",");
          out.print("  inputSchema:", out.importSchema(method.input), ",");
          out.print("  outputSchema:", out.importSchema(method.output), ",");
          out.print("}, codec);");
          out.print();
        }

        out.print(out.export("interface", `${name}Handler`), " {");
        for (const method of service.methods) {
          out.print("  ", method.localName, "(request:", out.importShape(method.input),
            ", context:", Context, "): Promise<", out.importShape(method.output), ">;");
        }
        out.print("}");
        out.print();
        out.print(out.export("class", `${name}Client`), " {");
        out.print("  #transport: ", runtime("RpcClient"), ";");
        out.print("  private constructor(transport:", runtime("RpcClient"), ") { this.#transport = transport; }");
        out.print();
        out.print("  static bind(context:", Context, ", binding:string, name:string): ", name, "Client {");
        out.print("    return new ", name, "Client(new ", runtime("RpcClient"), "(context, binding, name));");
        out.print("  }");
        for (const method of service.methods) {
          out.print();
          out.print("  ", method.localName, "(request:", Initializer, "<typeof ", out.importSchema(method.input, true),
            ">, options:", Options, " = {}): Promise<", out.importShape(method.output), "> {");
          out.print("    return this.#transport.call(_", name, "_", method.localName,
            ", request, options) as Promise<", out.importShape(method.output), ">;");
          out.print("  }");
        }
        out.print("}");
        out.print();
        out.print(out.export("const", `${name}Registration`), ": ", Registration, " = ", runtime("createRegistration"), "([", rules.join(", "), "]);");
        out.print();
      }
    }
  },
});

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) runNodeJs(plugin);
