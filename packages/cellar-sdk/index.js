/** Cellar's buffered HTTP wire representation. No Node or browser globals. */
export function bytesToBase64(bytes) {
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  let result = "";
  for (let i = 0; i < bytes.length; i += 3) {
    const value = (bytes[i] << 16) | ((bytes[i + 1] ?? 0) << 8) | (bytes[i + 2] ?? 0);
    result += alphabet[value >>> 18] + alphabet[(value >>> 12) & 63]
      + (i + 1 < bytes.length ? alphabet[(value >>> 6) & 63] : "=")
      + (i + 2 < bytes.length ? alphabet[value & 63] : "=");
  }
  return result;
}
export function utf8(text) {
  const encoded = encodeURIComponent(text.toWellFormed());
  const bytes = [];
  for (let i = 0; i < encoded.length; i++) {
    if (encoded[i] === "%") { bytes.push(parseInt(encoded.slice(i + 1, i + 3), 16)); i += 2; }
    else bytes.push(encoded.charCodeAt(i));
  }
  return new Uint8Array(bytes);
}
export function response(body = "", {status = 200, headers = []} = {}) {
  return {status, headers, body: bytesToBase64(typeof body === "string" ? utf8(body) : body)};
}
export function jsonResponse(value, {status = 200, headers = []} = {}) {
  return response(JSON.stringify(value), {status, headers: [["content-type", "application/json"], ...headers]});
}
export function request(url, {method = "GET", headers = [], body = ""} = {}) {
  return {url, method, headers, body: bytesToBase64(typeof body === "string" ? utf8(body) : body)};
}
