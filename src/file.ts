import { envConfig } from "@cascateer/lib";
import { createHash } from "crypto";
import { createReadStream } from "fs";
import { readFile } from "fs/promises";
import { lookup } from "mime-types";
import { extname, relative, resolve } from "path";

const { DATABASE_FILE_BASE_URL = "files" } = envConfig();

export class File {
  static readonly BASE_URL = DATABASE_FILE_BASE_URL;

  static fromPath = (path: string) => new File(relative(this.BASE_URL, path));

  static fromName = (name: string) =>
    this.fromPath(resolve(this.BASE_URL, name));

  constructor(public name: string) {}

  get path() {
    return resolve(File.BASE_URL, this.name);
  }

  get extname() {
    return extname(this.path);
  }

  static hash(path: string) {
    return new Promise<string>((resolve, reject) => {
      const hash = createHash("md5");

      createReadStream(path)
        .on("error", (error) => reject(error.message))
        .on("data", (data) => hash.update(data))
        .on("end", () => resolve(hash.digest("hex")));
    });
  }

  async hash() {
    return File.hash(this.path);
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
