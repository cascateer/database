import { createHash } from "crypto";
import { createReadStream } from "fs";
import { Ora } from "ora";
import { extname, relative, resolve } from "path";
import { defaults } from "./defaults";
import { FileTable } from "./types";

export class File {
  static BASE_URL = defaults.FILE_BASE_URL;

  static fromPath = (path: string) => new File(relative(this.BASE_URL, path));

  static fromUrl = (table: FileTable, url: string, spinner?: Ora) =>
    table
      .accessSome([url], spinner)
      .then(([{ name, checksum }]) => new File(name).verified(checksum));

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

  toString(): string {
    return this.path;
  }
}
