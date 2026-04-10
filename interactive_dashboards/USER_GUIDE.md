# User Guide

This guide is written for someone who is completely new to crypto, trading, and triangular arbitrage.

You do not need trading experience to explore this app. Think of it as a visualization tool that helps you see price relationships between cryptocurrencies.

## What This App Does

The app has two views:

- `Live`: shows price relationships happening right now using Binance US market data
- `Historical`: shows patterns from stored past data between `2017-01-01` and `2022-12-31`

The app is for learning and analysis.

It is not a trading bot.

It is not financial advice.

## What "Triangular Arbitrage" Means

You do not need to memorize the term, but here is the simple idea:

1. Start with one currency.
2. Exchange it for a second currency.
3. Exchange that for a third currency.
4. Exchange that back to the first currency.

If you end up with more than you started with, that is a possible arbitrage opportunity.

Example:

- Start with USD
- Buy BTC
- Trade BTC for ETH
- Trade ETH back to USD

If that loop would leave you with more USD than you started with, the app marks that as a potentially profitable cycle.

In real markets, this is hard to execute because prices move quickly and fees matter. That is why this app should be treated as an educational dashboard.

## The Two Modes

### Live Mode

Use this when you want to watch the market in real time.

The app:

- downloads available trading pairs from Binance US
- listens to live price updates
- checks whether any 3-currency loops look profitable

### Historical Mode

Use this when you want to study past patterns.

The app:

- loads saved historical data from the backend
- summarizes how many opportunities existed in a date range
- shows how the currency network changed over time

## First-Time Quick Start

If you are brand new, this is the easiest way to explore the app:

1. Open the app.
2. Start in `Live` mode.
3. Wait 10 to 30 seconds for data to begin filling in.
4. Watch the `Messages/sec`, `Price Map`, and `Opportunities` cards.
5. Look at the graph and the `Live Triangles` panel.
6. Click a few cards and graph nodes to see the extra detail panels.
7. Switch to `Historical` mode to compare what the market looked like in the past.

If live mode feels too busy, switch to historical mode first. It is slower and easier to understand.

## How To Switch Between Modes

There is a mode toggle in the header.

- `Live` gives you streaming data and live detection
- `Historical` gives you date filters and time-based playback

The app remembers your last selected mode in the browser.

## Live Mode, Explained Simply

Live mode tries to answer this question:

"If I followed a 3-step currency loop right now, would the prices suggest a profit?"

### What You Will See

There are three main sections:

1. A control bar at the top
2. A row of summary cards
3. A main area with a graph on the left and an opportunity list on the right

### Top Control Bar

These controls change how the app looks at live market data.

`Connection Status`

- Shows whether the app is connected to the live market stream
- If it says disconnected or error, use `Reconnect`

`Pause / Resume`

- `Pause` stops the app from processing new live price updates
- `Resume` starts processing again

`Clear`

- Removes the opportunities currently shown in the session
- This is useful if you want to "start fresh" and watch new events appear

`Fee %`

- This tells the app how much trading cost to assume for each trade step
- Higher fees make opportunities less likely to appear profitable
- If you are unsure, leave it at the default

`Min Profit`

- This is the minimum profit percentage the app should show as a real opportunity
- Higher value = fewer, stricter results
- Lower value = more results

`Notional $`

- This is the starting dollar amount used in the calculation
- You can think of it as "How much money am I pretending to start with?"

`Stale`

- This controls how long an opportunity stays highlighted in the graph
- A smaller value makes old highlights disappear sooner

`Near Misses`

- When this is on, the app also shows cycles that are close to profitable but not quite there
- This is helpful for learning because it shows "almost" opportunities too

### Summary Cards

These cards give you a quick overview of what the app is monitoring.

`Base Coins`

- How many important currencies are being used as anchors for the live search
- Clicking this opens a selector so you can change them

`Pairs`

- How many trading pairs are currently included
- Example: `BTC/USD` is a pair

`Possible Triangles`

- How many 3-currency loops the app was able to build from the available pairs

`Messages/sec`

- How many live price updates the app is receiving each second
- Higher number usually means the live stream is active and healthy

`Price Map`

- How many pairs have already received at least one price update
- This often grows soon after live mode starts

`Opportunities`

- How many profitable live detections have happened in the current session
- This number resets if you press `Clear`

### The Live Network Graph

The graph is a picture of the currency relationships.

`Circles`

- Each circle is a currency, such as `BTC`, `ETH`, or `USD`

`Lines`

- Each line is a trading pair connecting two currencies

How to read it:

- green usually means profitable activity
- amber means near-miss activity
- dimmer lines usually mean older activity
- gray means no recent interesting event

What you can do:

- click a node to select it
- drag nodes to move them
- zoom in and out
- turn on `Active only` to hide the quiet parts of the network

### The Live Triangles Panel

This panel lists grouped live opportunities.

Each row tells you:

- which 3 currencies are involved
- whether the opportunity is in the forward or reverse direction
- how recently it happened
- the total estimated volume
- the average profit for that group

If you click a row, it expands to show the underlying grouped entries.

If the panel is empty, that does not mean the app is broken. It may simply mean:

- there is no profitable loop right now
- your `Min Profit` is set too high
- you need to wait a bit longer for more live data

## Historical Mode, Explained Simply

Historical mode helps answer questions like:

- Which currencies showed up often in the past?
- When were opportunities more common?
- How did the network change over time?

### What You Will See

There are four main parts:

1. Date controls in the header
2. Summary cards
3. A network graph
4. A chart showing activity over time

### Date Controls

At the top of the page in historical mode, you can choose:

- `Start` date
- `End` date
- `Interval`

`Interval` means how the data should be grouped:

- `Day`: most detailed
- `Month`: easiest for most people
- `Year`: broad overview

If you are new, start with `Month`.

### Summary Cards

These cards describe the selected date range.

`Total Opportunities`

- How many opportunities were found in the selected time period

`Average Profit`

- The average profit percentage across the selected period

`Max Profit`

- The single highest profit percentage seen in that period

`Total Volume`

- The total dollar volume associated with the opportunities

`Unique Triangles`

- How many different 3-currency loops appeared

`Currencies`

- How many different currencies appeared in the selected data

### Historical Network Graph

This graph shows the currency network for the selected time bucket.

It helps you see:

- which currencies were more central
- which connections appeared more often
- how the network changed over time

You can:

- click a node to open more details
- zoom and drag the graph
- use the playback controls to move through time

### Playback Controls

These controls sit below the graph.

You can:

- step backward one snapshot
- play the timeline like an animation
- pause
- step forward one snapshot
- drag the slider to a specific time
- change playback speed

This lets you watch the network evolve over time.

### Graph Filters

The graph controls let you reduce clutter.

`Min Frequency`

- Hides currencies and links that appear too rarely

`Min Profit %`

- Hides low-profit data from the graph

If the graph feels too crowded, increase `Min Frequency`.

### Detail Drawer

When you click a currency in the historical graph, a side drawer opens.

It shows:

- how many triangle groups include that currency
- the best average profit among those results
- the total volume across those results
- a table of triangle rows

Each table row includes:

- the three currencies in the loop
- how many times it appeared
- average profit
- max profit
- total volume
- when it was last seen

This is a good way to answer questions like:

- "What loops involved BTC?"
- "Was ETH part of many strong opportunities?"

## A Simple Way To Explore The App

If you are unsure where to begin, try this path:

### Beginner Path 1: Live Mode

1. Open `Live` mode.
2. Wait for the connection to settle.
3. Confirm `Messages/sec` is above zero.
4. Watch `Price Map` increase.
5. Leave `Fee %` and `Notional $` at the defaults.
6. Turn on `Near Misses` if the list looks empty.
7. Lower `Min Profit` a little if you want to see more activity.

### Beginner Path 2: Historical Mode

1. Switch to `Historical`.
2. Set the interval to `Month`.
3. Keep the full date range first.
4. Look at the summary cards.
5. Press play below the graph.
6. Click one or two large nodes to inspect them.

## Plain-Language Meanings Of Common Terms

`Currency`

- A digital asset or money unit, such as `BTC`, `ETH`, or `USD`

`Pair`

- A market between two currencies
- Example: `BTCUSD`

`Triangle`

- A 3-currency loop

`Profit %`

- The estimated percentage gain or loss for a loop

`Volume`

- The approximate dollar amount associated with the opportunity

`Frequency`

- How often something appeared

`Node`

- A circle in the graph

`Link`

- A line connecting two nodes

`Near Miss`

- A loop that almost looked profitable, but not enough to count as one

`Stale`

- An older event that is still shown, but no longer considered fresh

## Troubleshooting

### "Nothing is showing in Live mode"

Check these first:

- make sure the app says it is connected
- wait 10 to 30 seconds
- make sure `Messages/sec` is not zero
- turn on `Near Misses`
- lower `Min Profit`

### "The graph is too crowded"

Try:

- turning on `Active only` in live mode
- increasing `Min Frequency` in historical mode
- zooming in
- selecting a smaller time range in historical mode

### "Historical mode is not loading"

This usually means a setup issue, not a user mistake in the interface.

Possible causes:

- backend is not running
- BigQuery configuration is missing
- backend credentials are invalid

### "I do not understand whether green always means money I could really make"

Not necessarily.

Green means the app's current calculation logic thinks the loop looks profitable based on the data it has.

Real trading may still fail because of:

- delay
- fees
- low liquidity
- price movement

## Best Practices For New Users

- Start with defaults before changing settings
- Use `Month` in historical mode first
- Turn on `Near Misses` if live mode feels empty
- Use the graphs as learning tools, not trading instructions
- Click around often; many cards and graph items reveal more detail

## Final Reminder

This app is best used to understand patterns, relationships, and market structure.

You do not need to be an expert to use it. The easiest way to learn is:

1. start with the defaults
2. watch the graph and cards change
3. click on things
4. compare live behavior with historical behavior
