// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

var CellarApp = {
  cells: {
    Counter: class {
      async fetch(_req, ctx) {
        const file = await ctx.fs.open("/note", {
          access: "read_write",
          create: true,
        });
        await file.writeAll(utf8("abc"));
        await file.close();
        for (const action of [
          () => file.writeAll(new Uint8Array()),
          () => file.readExact(0),
        ]) {
          let refused = false;
          try {
            await action();
          } catch (error) {
            if (
              error.code !== "Filesystem" ||
              error.errno !== "EBADF" ||
              error.outcome !== "not_committed"
            )
              throw error;
            refused = true;
          }
          if (!refused) throw new Error("closed empty helper succeeded");
        }
        const data = await ctx.fs.readFile("/note");
        if (data.length !== 3 || data[0] !== 97)
          throw new Error("helper changed data");
        await ctx.fs.sync();
        return response("ok");
      }
    },
  },
  async fetch(req, ctx) {
    const result = await ctx.cell("COUNTERS", "small").fetch(request(req.url));
    return response(result.body, {
      status: result.status,
      headers: result.headers,
    });
  },
};
