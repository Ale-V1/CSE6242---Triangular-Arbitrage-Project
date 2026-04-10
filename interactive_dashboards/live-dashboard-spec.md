# Live Dashboard Specification

This document reflects the current implementation in the repository.

## Scope

Live mode is frontend-only:

- no backend is required
- exchange metadata comes from Binance US REST
- price updates come from Binance US WebSocket streams
- arbitrage scanning runs in a Web Worker

## Default Endpoints

From [env.ts](/Users/silvy/code/school/cse6242/apps/frontend/src/lib/env.ts):

- REST base: `https://api.binance.us/api/v3`
- WebSocket base: `wss://stream.binance.us:9443/stream`

These can be overridden with:

- `VITE_BINANCE_REST_URL`
- `VITE_BINANCE_WS_URL`

## Pair Selection

Exchange metadata is fetched from `exchangeInfo`.

The app keeps only symbols where:

- `status === "TRADING"`
- either `baseAsset` or `quoteAsset` is in the selected coin set

Default selected coin set:

- `USDT`
- `USD`
- `USDC`
- `BTC`
- `ETH`

This differs from some older docs that mentioned `BNB` or `BUSD`; those are not the current defaults in code.

## Triangle Construction

The app:

1. builds an undirected adjacency map from filtered pairs
2. enumerates three-currency cycles
3. deduplicates them using a sorted canonical key like `BTC-ETH-USDT`

Each triangle stores:

- canonical key
- three currencies
- three pair symbols

## WebSocket Behavior

The WebSocket hook:

- subscribes to multiplexed `@bookTicker` streams
- caps subscriptions at `300` streams
- tracks messages per second
- keeps a message log capped at `500` entries
- retries reconnect up to `5` times with exponential backoff

Connection statuses used in the UI:

- `connecting`
- `connected`
- `disconnected`
- `error`

## Worker Behavior

The worker receives:

- initial triangle list
- current live config
- price updates

The worker maintains:

- a price map keyed by symbol
- current config
- triangle list

On each relevant price update, it checks triangles in both directions.

The current live config fields are:

- `fee`
- `minProfit`
- `notional`
- `nearMissFloor`

## Opportunity Handling

Raw worker results are stored in the main thread and then deduplicated.

Important current limits:

- max raw opportunities kept: `1000`
- max deduplicated entries kept: `1000`

Grouping logic:

- dedup key = triangle key + direction + profit rounded to 2 decimals
- if a matching entry stays fresh within the current stale window, counts and volume are merged
- if it becomes stale, a new entry is created

Categories:

- `profitable`
- `near-miss`

Display note:

- the large `Opportunities` stat increments only for `profitable` events
- near-miss entries can still appear in the feed and graph when enabled

## Controls

The live controls bar exposes:

- connection status
- reconnect
- pause/resume
- clear
- fee slider
- minimum profit slider
- notional input
- stale slider
- near-miss toggle
- near-miss floor slider

Current defaults:

- `fee = 0.1`
- `minProfit = 0.05`
- `notional = 1000`
- `nearMissFloor = -0.5`
- `staleMinutes = 5`
- `showNearMisses = false`
- `showActiveOnly = true`

Current input ranges in the UI:

- fee: `0.00` to `1.00`
- min profit: `0.00` to `2.00`
- notional: `1` to `100000`
- stale minutes: `1` to `30`
- near-miss floor slider: `-1.00` to `0.00`

## Pause, Clear, And Persistence

Pause behavior:

- the WebSocket remains connected
- incoming messages are not forwarded to the worker while paused

Clear behavior:

- clears opportunity history
- resets total profitable count
- resets session start time
- does not change live config

Persisted in `localStorage`:

- live config
- selected coins
- `showNearMisses`
- `staleMinutes`
- `showActiveOnly`

## Stats Row

The live dashboard renders six cards:

- `Base Coins`
- `Pairs`
- `Possible Triangles`
- `Messages/sec`
- `Price Map`
- `Opportunities`

Interactive cards open drawers for:

- coin selector
- pairs
- triangles
- stream log
- price map

## Graph

The live graph is built from:

- filtered exchange pairs
- detected opportunities

Behavior:

- nodes represent currencies
- links represent pair symbols
- clicking a node marks it as selected in shared app state
- `Active only` filters the graph to links present in the current highlighted set

Highlight logic is derived from the deduplicated opportunity list:

- profitable active edges have highest priority
- near-miss active edges are below that
- stale edges are lower priority

## Feed

The opportunity feed groups deduplicated entries by:

- triangle key
- direction

Each group shows:

- currency path
- direction
- relative time
- aggregated USD volume
- average profit
- count of grouped rows when more than one exists

Rows can be expanded to show grouped child entries.

