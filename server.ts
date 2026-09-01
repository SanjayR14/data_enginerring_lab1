import express, { Request, Response } from 'express';
import { createServer as createViteServer } from 'vite';
import path from 'path';
import { fileURLToPath } from 'url';
import { spawn, ChildProcess } from 'child_process';
import http from 'http';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = 3000;
const BACKEND_PORT = 8000;
const BACKEND_HOST = process.env.BACKEND_HOST || '127.0.0.1';
let pythonProcess: ChildProcess | null = null;

// Function to start FastAPI Python process
function startFastAPIBackend() {
  console.log('Starting FastAPI backend process on port 8000...');
  
  pythonProcess = spawn('python3', [
    '-m', 'uvicorn',
    'backend.app.main:app',
    '--host', '0.0.0.0',
    '--port', '8000'
  ], {
    env: { ...process.env, PYTHONPATH: '.' },
    stdio: 'inherit'
  });

  pythonProcess.on('error', (err) => {
    console.error('Failed to start FastAPI process:', err);
  });

  pythonProcess.on('exit', (code, signal) => {
    console.log(`FastAPI backend process exited with code ${code} / signal ${signal}`);
  });
}

async function startServer() {
  // Start Python FastAPI background process
  startFastAPIBackend();

  const app = express();

  // Proxy /api requests to FastAPI server at port 8000
  app.use('/api', (req: Request, res: Response) => {
    const options = {
      hostname: BACKEND_HOST,
      port: BACKEND_PORT,
      path: `/api${req.url}`,
      method: req.method,
      headers: {
        ...req.headers,
        host: `${BACKEND_HOST}:${BACKEND_PORT}`,
      },
    };

    const proxyReq = http.request(options, (proxyRes) => {
      res.writeHead(proxyRes.statusCode || 500, proxyRes.headers);
      proxyRes.pipe(res, { end: true });
    });

    proxyReq.on('error', (err) => {
      console.error('Error proxying to FastAPI backend:', err.message);
      res.status(503).json({
        detail: 'FastAPI Backend service starting up... Please refresh in a moment.'
      });
    });

    req.pipe(proxyReq, { end: true });
  });

  // Vite middleware setup for SPA frontend
  if (process.env.NODE_ENV !== 'production') {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), 'dist');
    app.use(express.static(distPath));
    app.get('*', (req: Request, res: Response) => {
      res.sendFile(path.join(distPath, 'index.html'));
    });
  }

  app.listen(PORT, '0.0.0.0', () => {
    console.log(`Cloud Cost Intelligence Platform listening on http://0.0.0.0:${PORT}`);
  });
}

startServer();
