// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

/** Native backup-gateway subset; does not declare complete R2 conformance. */
export interface R2Bucket {
  head(key: string): Promise<R2Object | null>;
  get(
    key: string,
    options?: { range?: { offset?: number; length?: number; suffix?: number } },
  ): Promise<R2ObjectBody | null>;
  delete(key: string): Promise<void>;
  createMultipartUpload(
    key: string,
    options?: R2MultipartOptions,
  ): Promise<R2MultipartUpload>;
  resumeMultipartUpload(key: string, uploadId: string): R2MultipartUpload;
}
export interface R2HTTPMetadata {
  contentType?: string;
  contentLanguage?: string;
  contentDisposition?: string;
  contentEncoding?: string;
  cacheControl?: string;
  cacheExpiry?: Date;
}
export interface R2MultipartOptions {
  httpMetadata?: R2HTTPMetadata;
  customMetadata?: Record<string, string>;
}
export interface R2UploadedPart {
  readonly partNumber: number;
  readonly etag: string;
}
export interface R2MultipartUpload {
  readonly key: string;
  readonly uploadId: string;
  uploadPart(
    partNumber: number,
    value:
      | ReadableStream<Uint8Array>
      | ArrayBuffer
      | ArrayBufferView
      | string
      | Blob,
  ): Promise<R2UploadedPart>;
  /** This profile requires consecutive part numbers beginning at one. */
  complete(parts: readonly R2UploadedPart[]): Promise<R2Object>;
  abort(): Promise<void>;
}
export interface R2Object {
  readonly key: string;
  readonly version: string;
  readonly size: number;
  readonly etag: string;
  readonly httpEtag: string;
  readonly uploaded: Date;
  readonly httpMetadata: R2HTTPMetadata;
  readonly customMetadata: Record<string, string>;
  readonly checksums: Partial<
    Record<"md5" | "sha1" | "sha256" | "sha384" | "sha512", ArrayBuffer>
  >;
  readonly storageClass: "Standard" | "InfrequentAccess";
  readonly range?: { offset: number; length: number };
  writeHttpMetadata(headers: Headers): void;
}
export interface R2ObjectBody extends R2Object {
  readonly body: ReadableStream<Uint8Array>;
  readonly bodyUsed: boolean;
  arrayBuffer(): Promise<ArrayBuffer>;
  text(): Promise<string>;
  json<T = unknown>(): Promise<T>;
  blob(): Promise<Blob>;
}
