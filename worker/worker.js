/**
 * Cloudflare Worker — Gemini API Proxy for hisseiny.github.io
 *
 * Accepts Ollama-compatible requests from the portfolio chatbot,
 * translates them to Gemini API format, and streams responses back
 * in Ollama-compatible NDJSON. The API key never touches the frontend.
 *
 * Setup:
 *   1. npx wrangler secret put GEMINI_API_KEY
 *   2. npx wrangler deploy
 */

const ALLOWED_ORIGINS = [
  'https://hisseiny.github.io',
  'http://localhost:8080',
  'http://127.0.0.1:8080',
];

const CORS_HEADERS = {
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Max-Age': '86400',
};

function corsHeaders(request) {
  const origin = request.headers.get('Origin') || '';
  const allowed = ALLOWED_ORIGINS.includes(origin) ? origin : ALLOWED_ORIGINS[0];
  return { ...CORS_HEADERS, 'Access-Control-Allow-Origin': allowed };
}

// Rate limiter: 20 requests per minute per IP
const rateLimitMap = new Map();
const RATE_LIMIT = 20;
const RATE_WINDOW = 60000;

function isRateLimited(ip) {
  const now = Date.now();
  const entry = rateLimitMap.get(ip);
  if (!entry || now - entry.start > RATE_WINDOW) {
    rateLimitMap.set(ip, { start: now, count: 1 });
    return false;
  }
  entry.count++;
  return entry.count > RATE_LIMIT;
}

export default {
  async fetch(request, env) {
    const cors = corsHeaders(request);

    // Preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: cors });
    }

    // Health check — mimics Ollama /api/tags so frontend works unchanged
    const url = new URL(request.url);
    if (url.pathname === '/api/tags' && request.method === 'GET') {
      return new Response(JSON.stringify({
        models: [{ name: 'gemini-2.0-flash', modified_at: new Date().toISOString() }]
      }), {
        headers: { 'Content-Type': 'application/json', ...cors }
      });
    }

    // Chat endpoint
    if (url.pathname === '/api/chat' && request.method === 'POST') {
      // Rate limiting
      const ip = request.headers.get('CF-Connecting-IP') || 'unknown';
      if (isRateLimited(ip)) {
        return new Response(JSON.stringify({ error: 'Rate limited. Try again in a minute.' }), {
          status: 429, headers: { 'Content-Type': 'application/json', ...cors }
        });
      }

      // Origin check
      const origin = request.headers.get('Origin') || '';
      if (!ALLOWED_ORIGINS.includes(origin)) {
        return new Response(JSON.stringify({ error: 'Forbidden' }), {
          status: 403, headers: { 'Content-Type': 'application/json', ...cors }
        });
      }

      // Parse Ollama-format request
      let body;
      try {
        body = await request.json();
      } catch {
        return new Response(JSON.stringify({ error: 'Invalid JSON' }), {
          status: 400, headers: { 'Content-Type': 'application/json', ...cors }
        });
      }

      const messages = body.messages || [];
      const systemMsg = messages.find(m => m.role === 'system');
      const chatMsgs = messages.filter(m => m.role !== 'system');

      // Ensure at least one user message
      if (chatMsgs.length === 0) {
        return new Response(JSON.stringify({ error: 'No messages' }), {
          status: 400, headers: { 'Content-Type': 'application/json', ...cors }
        });
      }

      // Convert to Gemini format
      const geminiBody = {
        contents: chatMsgs.map(m => ({
          role: m.role === 'assistant' ? 'model' : 'user',
          parts: [{ text: m.content }]
        })),
        generationConfig: {
          maxOutputTokens: 300,
          temperature: 0.7,
        }
      };

      if (systemMsg) {
        geminiBody.systemInstruction = { parts: [{ text: systemMsg.content }] };
      }

      // Call Gemini API (streaming SSE)
      const apiKey = env.GEMINI_API_KEY;
      if (!apiKey) {
        return new Response(JSON.stringify({ error: 'API key not configured' }), {
          status: 500, headers: { 'Content-Type': 'application/json', ...cors }
        });
      }

      const geminiUrl = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:streamGenerateContent?alt=sse&key=${apiKey}`;

      let geminiRes;
      try {
        geminiRes = await fetch(geminiUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(geminiBody),
        });
      } catch {
        return new Response(JSON.stringify({ error: 'Gemini API unreachable' }), {
          status: 502, headers: { 'Content-Type': 'application/json', ...cors }
        });
      }

      if (!geminiRes.ok) {
        const errText = await geminiRes.text().catch(() => 'Unknown error');
        return new Response(JSON.stringify({ error: 'Gemini error: ' + geminiRes.status }), {
          status: 502, headers: { 'Content-Type': 'application/json', ...cors }
        });
      }

      // Stream Gemini SSE → Ollama NDJSON
      const { readable, writable } = new TransformStream();
      const writer = writable.getWriter();
      const encoder = new TextEncoder();

      (async () => {
        const reader = geminiRes.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop() || '';

            for (const line of lines) {
              if (!line.startsWith('data: ')) continue;
              const data = line.slice(6).trim();
              if (!data || data === '[DONE]') continue;

              try {
                const parsed = JSON.parse(data);
                const text = parsed.candidates?.[0]?.content?.parts?.[0]?.text || '';
                if (text) {
                  await writer.write(encoder.encode(
                    JSON.stringify({ message: { role: 'assistant', content: text }, done: false }) + '\n'
                  ));
                }
              } catch {}
            }
          }

          // Final done signal
          await writer.write(encoder.encode(
            JSON.stringify({ message: { role: 'assistant', content: '' }, done: true }) + '\n'
          ));
        } finally {
          writer.close();
        }
      })();

      return new Response(readable, {
        headers: { 'Content-Type': 'application/x-ndjson', 'Cache-Control': 'no-cache', ...cors }
      });
    }

    return new Response('Not found', { status: 404, headers: cors });
  }
};
