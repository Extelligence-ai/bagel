// Connect only to the local Bagel server; never expose or upload it to Smithery.
const port = Number(process.argv[2] || 8000);
if (!Number.isInteger(port) || port < 1 || port > 65535) {
  console.error('Bagel port must be an integer between 1 and 65535.');
  process.exit(1);
}
process.argv = [process.argv[0], process.argv[1],
  `http://127.0.0.1:${port}/sse`, '--transport', 'sse-only', '--allow-http'];
await import('./node_modules/mcp-remote/dist/proxy.js');
