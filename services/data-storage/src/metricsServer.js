const http = require('http');
const config = require('./config');
const { getMetrics } = require('./metrics');
const { drainBuffer } = require('./db');

function startMetricsServer() {
  const server = http.createServer(async (req, res) => {
    if (req.url === '/metrics' && req.method === 'GET') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(getMetrics()));
      return;
    }

    if (req.url === '/drain' && req.method === 'POST') {
      try {
        const flushed = await drainBuffer();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ flushed, metrics: getMetrics() }));
      } catch (error) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: error.message }));
      }
      return;
    }

    if (req.url === '/health' && req.method === 'GET') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok', broker_type: config.brokerType }));
      return;
    }

    res.writeHead(404);
    res.end();
  });

  server.listen(config.metricsPort, () => {
    console.log(`Metrics server listening on port ${config.metricsPort}`);
  });

  return server;
}

module.exports = { startMetricsServer };
