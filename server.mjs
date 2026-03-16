import express from 'express';
import { exec } from 'child_process';
import path from 'path';

// Basic Express server to bridge the dashboard UI and the agent
//
// Provides three API routes:
//   GET /api/agent/status   - runs a health check on the agent and returns the result
//   POST /api/agent/run     - runs the agent once or with --dry-run and returns the output
//   POST /api/agent/chat    - echoes a simple reply using the Anthropic API (or an error if not configured)

const app = express();
const PORT = process.env.PORT || 3000;
app.use(express.json());

// Serve the dashboard HTML at the root path
app.get('/', (req, res) => {
  res.sendFile(path.join(process.cwd(), 'nowcastiq_dashboard_with_agent.html'));
});

// Health/status endpoint runs the agent in health check mode
app.get('/api/agent/status', (req, res) => {
  exec('node nowcastiq-agent.mjs --health', (error, stdout, stderr) => {
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    res.json({ status: stdout.trim() });
  });
});

// Run endpoint triggers a full or dry run of the agent
app.post('/api/agent/run', (req, res) => {
  const dryRun = req.body && req.body.dryRun;
  const cmd = dryRun ? 'node nowcastiq-agent.mjs --dry-run' : 'node nowcastiq-agent.mjs';
  exec(cmd, { maxBuffer: 10 * 1024 * 1024 }, (error, stdout, stderr) => {
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    res.json({ result: stdout.trim() });
  });
});

// Simple chat endpoint. In a production deployment this would call the Anthropic API.
// Here we return a placeholder response or an error if the API key is missing.
app.post('/api/agent/chat', async (req, res) => {
  const message = req.body && req.body.message;
  if (!message) {
    return res.status(400).json({ error: 'Missing message' });
  }
  // If an Anthropic API key is provided, attempt to call it
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (apiKey) {
    try {
      const fetchResponse = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'x-api-key': apiKey,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          model: 'claude-3-sonnet-20240229',
          messages: [
            { role: 'user', content: message }
          ],
          max_tokens: 100,
        }),
      });
      const data = await fetchResponse.json();
      const reply = (data && data.content && data.content[0] && data.content[0].text) || '';
      return res.json({ reply });
    } catch (err) {
      return res.status(500).json({ error: err.message });
    }
  }
  // Fallback if no API key configured
  res.json({ reply: `Echo: ${message}` });
});

// Start the server
app.listen(PORT, () => {
  console.log(`NowcastIQ dashboard server listening on port ${PORT}`);
});