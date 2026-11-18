(function () {
  const send = (stage, message) =>
    fetch("/__load-diagnostic", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ stage, message, ua: navigator.userAgent }),
      keepalive: true,
    }).catch(() => {});
  send("script-start", "secure=" + window.isSecureContext);
  window.addEventListener(
    "error",
    (e) => send("error", e.message + " " + e.filename + ":" + e.lineno),
    true,
  );
  window.addEventListener("unhandledrejection", (e) =>
    send("rejection", String(e.reason?.stack || e.reason)),
  );
  window.addEventListener("load", () => send("window-load", ""));
  setTimeout(
    () =>
      send(
        "after-30s",
        "rootChildren=" + document.getElementById("root")?.childElementCount,
      ),
    30000,
  );
})();
// UUID v4 compatibility for HTTP access over a private VPN.
if (typeof globalThis.crypto.randomUUID !== "function") {
  Object.defineProperty(globalThis.crypto, "randomUUID", {
    value: function randomUUID() {
      const bytes = new Uint8Array(16);
      globalThis.crypto.getRandomValues(bytes);
      bytes[6] = (bytes[6] & 15) | 64;
      bytes[8] = (bytes[8] & 63) | 128;
      const hex = Array.from(bytes, (b) =>
        b.toString(16).padStart(2, "0"),
      ).join("");
      return (
        hex.slice(0, 8) +
        "-" +
        hex.slice(8, 12) +
        "-" +
        hex.slice(12, 16) +
        "-" +
        hex.slice(16, 20) +
        "-" +
        hex.slice(20)
      );
    },
    configurable: true,
  });
}
