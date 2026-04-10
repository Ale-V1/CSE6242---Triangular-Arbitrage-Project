# Triangular Arbitrage Visualization

This repository is a `pnpm` workspace with two apps:

- `apps/frontend`: a React + Vite dashboard
- `apps/backend`: an Express API for historical data

The project supports two modes:

- `Live` mode: real-time triangular arbitrage detection using Binance US market data
- `Historical` mode: a BigQuery-backed dashboard for precomputed historical opportunities

## Before You Start

This project does not use Python or a `requirements.txt`. Dependencies are managed with the repo's Node workspace files:

- `package.json`
- `pnpm-lock.yaml`

Install these tools first:

- Node.js `18+`
- `pnpm` `9+`

Check your versions:

```bash
node --version
pnpm --version
```

If you do not already have `pnpm`, the easiest setup is usually:

```bash
corepack enable
corepack prepare pnpm@latest --activate
pnpm --version
```

If `corepack` is unavailable, install `pnpm` globally:

```bash
npm install -g pnpm
pnpm --version
```

## Clone And Install

From the repository root, install all workspace dependencies:

```bash
pnpm install
```

## Project Layout

```text
cse6242/
├── apps/
│   ├── backend/    # Express API for historical mode
│   └── frontend/   # React/Vite dashboard
├── README.md
├── USER_GUIDE.md
├── historical-dashboard-spec.md
├── live-dashboard-spec.md
├── package.json
├── pnpm-lock.yaml
└── pnpm-workspace.yaml
```

## Choose How You Want To Run The App

### Option 1: Live Dashboard Only

Choose this if you want the real-time dashboard and do not need historical data.

You only need the frontend:

```bash
pnpm dev:frontend
```

Open:

- `http://localhost:5173`

What works in this mode:

- Live dashboard
- Binance US pair discovery
- Binance US WebSocket streaming
- Client-side arbitrage detection

What does not work in this mode:

- Historical dashboard API calls

### Option 2: Full App

Choose this if you want both live mode and historical mode.

Start the backend in one terminal:

```bash
pnpm dev:backend
```

Start the frontend in a second terminal:

```bash
pnpm dev:frontend
```

Open:

- Frontend: `http://localhost:5173`
- Backend health check: `http://localhost:3001/health`

You can also start both with one command:

```bash
pnpm dev
```

Using two terminals is usually easier when debugging startup issues.

## Environment Setup

### Frontend

The frontend works with its built-in defaults, so most users can skip this step.

Default frontend values:

```env
VITE_API_URL=/api
VITE_BINANCE_REST_URL=https://api.binance.us
VITE_BINANCE_WS_URL=wss://stream.binance.us:9443/stream
```

If you want to override them, create `apps/frontend/.env` from `apps/frontend/.env.example`.

On macOS or Linux:

```bash
cp apps/frontend/.env.example apps/frontend/.env
```

On Windows PowerShell:

```powershell
Copy-Item apps/frontend/.env.example apps/frontend/.env
```

Notes:

- In local development, Vite proxies `/api` to `http://localhost:3001`
- If you keep the defaults, you usually do not need a frontend `.env` file

### Backend

The backend is required for historical mode.

Create `apps/backend/.env` from `apps/backend/.env.example`.

On macOS or Linux:

```bash
cp apps/backend/.env.example apps/backend/.env
```

On Windows PowerShell:

```powershell
Copy-Item apps/backend/.env.example apps/backend/.env
```

Required values:

```env
PORT=3001
BIGQUERY_VIEW_ID=your-project.your_dataset.vw_triangle_opportunities_enriched
BIGQUERY_LOCATION=US
GOOGLE_APPLICATION_CREDENTIALS_EMAIL=service-account@your-project.iam.gserviceaccount.com
GOOGLE_APPLICATION_CREDENTIALS_KEY="-----BEGIN PRIVATE KEY-----\nREPLACE_ME\n-----END PRIVATE KEY-----\n"
```

You can also define the BigQuery view with separate values instead of `BIGQUERY_VIEW_ID`:

```env
BIGQUERY_PROJECT_ID=your-project
BIGQUERY_DATASET=your_dataset
BIGQUERY_VIEW=vw_triangle_opportunities_enriched
```

Important details:

- `BIGQUERY_VIEW_ID` must use `project.dataset.view` format
- The backend will fail at startup if BigQuery settings are missing or invalid
- The live dashboard does not need the backend
- The private key must keep the escaped `\n` sequences exactly as shown

## Available Scripts

Run these from the repo root:

```bash
pnpm dev
pnpm dev:backend
pnpm dev:frontend
pnpm build
pnpm typecheck
pnpm lint
pnpm format
pnpm check
```

What they do:

- `pnpm dev`: starts backend and frontend together
- `pnpm dev:backend`: starts the Express backend with file watching
- `pnpm dev:frontend`: starts the Vite dev server on port `5173`
- `pnpm build`: builds backend and frontend
- `pnpm typecheck`: runs TypeScript checks across both apps
- `pnpm lint`: runs Biome linting
- `pnpm format`: formats files with Biome
- `pnpm check`: runs Biome checks

## Build And Preview

Build both apps:

```bash
pnpm build
```

Build outputs:

- backend: `apps/backend/dist`
- frontend: `apps/frontend/dist`

Run the built backend:

```bash
pnpm --filter @cse6242/backend start
```

Preview the built frontend:

```bash
pnpm --filter @cse6242/frontend preview
```

The preview URL is usually:

- `http://localhost:4173`

## API Summary

The backend exposes:

- `GET /health`
- `GET /api/historical/summary`
- `GET /api/historical/graph`
- `GET /api/historical/graph-timeline`
- `GET /api/historical/triangles`
- `GET /api/historical/opportunities`

The frontend currently uses:

- `/summary`
- `/graph-timeline`
- `/triangles`

## Troubleshooting

### Historical mode is not working

Check these first:

- the backend is running
- `apps/backend/.env` exists
- BigQuery credentials are valid
- the configured BigQuery view exists and is accessible

### Backend crashes on startup

Check:

- `BIGQUERY_VIEW_ID` or `BIGQUERY_PROJECT_ID` and `BIGQUERY_DATASET`
- `GOOGLE_APPLICATION_CREDENTIALS_EMAIL`
- `GOOGLE_APPLICATION_CREDENTIALS_KEY`
- `BIGQUERY_LOCATION`

### Live mode shows no market data

Check:

- your internet connection
- whether Binance US endpoints are reachable from your network
- the browser console for WebSocket errors

### A port is already in use

Run the backend on a different port:

```bash
PORT=3002 pnpm dev:backend
```

Run the frontend on a different port:

```bash
pnpm --filter @cse6242/frontend dev -- --port 5174
```
