# Historical Dashboard Specification

This document reflects the current implementation in the repository, not an aspirational design.

## Scope

Historical mode is a frontend + backend feature:

- frontend: React app in `apps/frontend`
- backend: Express API in `apps/backend`
- data source: BigQuery view configured through environment variables

The frontend currently uses these backend endpoints:

- `GET /api/historical/summary`
- `GET /api/historical/graph-timeline`
- `GET /api/historical/triangles`

These backend endpoints also exist but are not used by the main dashboard screen:

- `GET /api/historical/graph`
- `GET /api/historical/opportunities`

## Defaults

- default start date: `2017-01-01`
- default end date: `2022-12-31`
- default interval: `month`
- default graph `minFrequency`: `10`
- default graph `minProfitPct`: `0`
- triangle drawer page size: `50`

## Backend Configuration

The backend reads:

- `PORT`
- `BIGQUERY_VIEW_ID`
- `BIGQUERY_PROJECT_ID`
- `BIGQUERY_DATASET`
- `BIGQUERY_VIEW`
- `BIGQUERY_LOCATION`
- `GOOGLE_APPLICATION_CREDENTIALS_EMAIL`
- `GOOGLE_APPLICATION_CREDENTIALS_KEY`

Startup behavior:

- if `BIGQUERY_VIEW_ID` is missing, the app tries to construct it from project + dataset + view
- if the final view identifier is missing or invalid, backend startup throws

## API Contract

### `GET /health`

Returns:

```json
{
  "status": "ok",
  "timestamp": "2026-04-06T12:00:00.000Z"
}
```

### `GET /api/historical/summary`

Query params:

- `startDate`
- `endDate`
- `bin` optional: `day | month | year`

Returns:

- totals across the selected date range
- top triangles by count
- optional `timeSeries` when `bin` is provided

### `GET /api/historical/graph-timeline`

Query params:

- `startDate`
- `endDate`
- `bin`: `day | month | year`
- `minFrequency`
- `minProfitPct`

Returns:

- `snapshots`: graph state per bucket
- `allNodes`
- `allLinks`
- metadata including snapshot count

This is the endpoint that powers the animated graph.

### `GET /api/historical/triangles`

Query params:

- `startDate`
- `endDate`
- `currency` required
- `sortBy`: `count | profit | volume`
- `limit`
- `offset`

Returns grouped triangle rows involving the selected currency.

### `GET /api/historical/opportunities`

Query params:

- `startDate`
- `endDate`
- `triangleKey` optional
- `minProfitPct` optional
- `limit`
- `offset`

This endpoint exists in the backend, but the current dashboard UI does not call it.

## Frontend Flow

State is held in `AppContext`:

- mode
- fetch range
- interval bin
- selected node
- detail drawer state

When the user changes the date range or interval:

- `useSummary()` refetches summary data
- `useGraphTimeline()` refetches snapshot data
- playback index resets to `0`

When the user clicks a graph node:

- the selected node is stored in context
- the detail drawer opens
- `useTriangles()` fetches triangles for that currency

## UI Behavior

### Header

Historical mode shows:

- start date input
- end date input
- interval select

The allowed date bounds in the UI are:

- minimum: `2017-01-01`
- maximum: `2022-12-31`

### Summary Cards

The summary area renders six cards:

- total opportunities
- average profit
- max profit
- total volume
- unique triangles
- unique currencies

If data is loading, skeleton placeholders are shown.

### Graph Area

The graph card contains:

- graph title
- graph controls
- network visualization
- playback controls

Graph controls expose:

- `Min Frequency`
- `Min Profit %`

These values are applied server-side through `graph-timeline` query parameters.

### Playback

Playback is client-side once snapshots are loaded.

Available controls:

- previous snapshot
- play/pause
- next snapshot
- slider scrub
- speed select: `0.5x`, `1x`, `2x`, `4x`

The slider uses a small debounce for smooth scrubbing.

### Detail Drawer

Current behavior:

- opens only from node clicks
- title format: `<NODE> Triangles`
- requests `sortBy=count`
- requests `limit=50`
- does not currently expose pagination controls in the UI
- does not currently drill into individual opportunity rows from the drawer

The drawer summary tiles show:

- total triangle groups
- best average profit among returned rows
- total volume across returned rows

## Query Validation

The backend validates query params with Zod.

Rules include:

- dates must be valid date strings
- `limit` is `1` to `1000`
- `offset` is `>= 0`
- `minFrequency` is an integer `>= 0`
- `minProfitPct` is a number `>= 0`

Validation failures return `400`.

## Caching

Historical frontend queries use TanStack Query with:

- `staleTime = 5 minutes`

This applies to:

- summary
- graph timeline
- triangles

