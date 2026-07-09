import { createHash } from "crypto";
import { createReadStream } from "fs";
import { readFile } from "fs/promises";
import { lookup } from "mime-types";
import { extname, relative, resolve } from "path";
import { defaults } from "./defaults";

export class File {
  static get BASE_URL() {
    return defaults.FILE_BASE_URL;
  }

  static fromPath = (path: string) => new File(relative(this.BASE_URL, path));

  constructor(public name: string) {}

  get path() {
    return resolve(File.BASE_URL, this.name);
  }

  get extname() {
    return extname(this.path);
  }

  async hash() {
    return new Promise<string>((resolve, reject) => {
      const hash = createHash("md5");

      createReadStream(this.path)
        .on("error", (error) => reject(error.message))
        .on("data", (data) => hash.update(data))
        .on("end", () => resolve(hash.digest("hex")));
    });
  }

  async verify(checksum?: string): Promise<boolean> {
    return (await this.hash()) === checksum;
  }

  async verified(checksum: string): Promise<File> {
    if (await this.verify(checksum)) {
      return this;
    }

    throw new Error();
  }

  async dataUrl() {
    return readFile(this.path).then(
      (data) =>
        `data:${lookup(this.path)};base64,${Buffer.from(data).toString("base64")}`,
    );
  }

  toString(): string {
    return this.path;
  }
}
