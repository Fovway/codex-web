import { createReadStream, createWriteStream } from "node:fs";
import { readdir, stat } from "node:fs/promises";
import { join } from "node:path";
import { pipeline } from "node:stream/promises";
import { createGzip } from "node:zlib";

const assets = "scratch/asar/webview/assets";
const entries = await readdir(assets, { withFileTypes: true, recursive: true });
const files = entries
  .filter((entry) => entry.isFile() && /\.(?:js|css)$/.test(entry.name))
  .map((entry) => join(entry.parentPath, entry.name));

await Promise.all(
  Array.from({ length: 4 }, async () => {
    for (let file = files.pop(); file; file = files.pop()) {
      if ((await stat(file)).size <= 1024) continue;
      await pipeline(
        createReadStream(file),
        createGzip(),
        createWriteStream(file + ".gz"),
      );
    }
  }),
);
