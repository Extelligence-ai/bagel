// Explicitly publishes a prebuilt local connector; never deploys a hosted server.
import { readFileSync } from 'node:fs';

const [bundlePath, ...extra] = process.argv.slice(2);
if (!bundlePath || extra.length || !process.env.SMITHERY_API_KEY) {
  console.error('Usage: SMITHERY_API_KEY=<publisher key> node publish.mjs <bundle.mcpb>');
  process.exit(1);
}
const readJSON = (path) => JSON.parse(readFileSync(new URL(path, import.meta.url), 'utf8'));
const manifest = readJSON('./bundle/manifest.json');
const tools = readJSON('./tools.json');
if (!Array.isArray(tools) || tools.length === 0 || tools.some(tool => !tool.inputSchema)) {
  throw new Error('tools.json must contain full MCP tools/list schemas');
}
if (JSON.stringify(tools.map(({ name, description }) => ({ name, description }))) !==
    JSON.stringify(manifest.tools)) {
  throw new Error('Manifest tool summaries and tools.json disagree');
}
const payload = {
  type: 'stdio',
  runtime: 'node',
  serverCard: { serverInfo: { name: manifest.name, version: manifest.version }, tools },
  configSchema: readJSON('./config-schema.json'),
};
const form = new FormData();
form.set('payload', JSON.stringify(payload));
form.set('bundle', new Blob([readFileSync(bundlePath)], { type: 'application/zip' }), 'server.mcpb');
const response = await fetch('https://api.smithery.ai/servers/extelligence-ai/bagel/releases', {
  method: 'PUT',
  headers: { Authorization: `Bearer ${process.env.SMITHERY_API_KEY}` },
  body: form,
});
console.log(response.status, await response.text());
if (!response.ok) process.exitCode = 1;
