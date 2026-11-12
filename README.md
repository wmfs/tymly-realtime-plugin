# tymly-realtime-plugin

Keeps open Tymly cards up to date as data changes.

It starts with **shared live feeds**. Instead of every open card running its own refresh loop, cards subscribe to a feed. Each feed (one per feed name and input, for example one per job being viewed) is refreshed once for all of its viewers, and a viewer's card is only written to when its data has changed.

## Using a live feed

In a state machine, after the card's first load and before the state that waits for the user:

```json
"SubscribeToLiveFeed": {
  "Type": "Task",
  "Resource": "module:subscribeToLiveFeed",
  "ResourceConfig": {
    "feed": "jobSummary",
    "fetchFunction": "myapp_liveJobSummary",
    "presentFunction": "myapp_presentLiveJobSummary",
    "intervalSeconds": 30
  },
  "Parameters": { "jobId.$": "$.jobId" },
  "ResultPath": "$.liveFeedSubscription",
  "Next": "AwaitingHumanInput"
}
```

| ResourceConfig | |
| --- | --- |
| `feed` | Name of the feed. Viewers with the same feed name and input (`Parameters`) share one feed. |
| `fetchFunction` | Blueprint function that gets the shared data, called once per refresh with the input as `event`. Returns `{ data, finished }`; `finished: true` completes every viewer's card with the data and drops the feed. |
| `presentFunction` | Optional blueprint function run for each viewer with the fetched data as `event` and `{ userId }` as `context`, for anything that depends on who is looking. |
| `intervalSeconds` | Optional. Otherwise `realtime_liveFeedIntervalSeconds` applies. |
| `viewerTimeoutSeconds` | Optional. Otherwise `realtime_liveFeedViewerTimeoutSeconds` applies. |

Keep `"ResultPath": "$.liveFeedSubscription"`, so open cards are re-subscribed if Tymly restarts.

Updates are merged into what the card already has, so a feed must send every field it shows, using `null` for one that's now empty. A field left out keeps its old value.

## Settings

Registry keys, read at the start of every refresh, so changing them in the database takes effect without a restart. On first boot each one is created from its environment variable if set, otherwise its default.

| Registry key | Environment variable | Default |
| --- | --- | --- |
| `realtime_liveFeedIntervalSeconds` | `LIVE_FEED_INTERVAL_SECONDS` | 30 |
| `realtime_liveFeedOffsetSeconds` | `LIVE_FEED_OFFSET_SECONDS` | 10 |
| `realtime_liveFeedViewerTimeoutSeconds` | `LIVE_FEED_VIEWER_TIMEOUT_SECONDS` | 3600 |
| `realtime_liveFeedRefreshTimeoutSeconds` | `LIVE_FEED_REFRESH_TIMEOUT_SECONDS` | 20 |
| `realtime_liveFeedTimestampRefreshSeconds` | `LIVE_FEED_TIMESTAMP_REFRESH_SECONDS` | 300 |
| `realtime_liveFeedEventRetentionDays` | `LIVE_FEED_EVENT_RETENTION_DAYS` | 30 |
| `realtime_liveFeedStatusRole` | `LIVE_FEED_STATUS_ROLE` | `realtime_admin` |
| `realtime_liveFeedDebug` | `LIVE_FEED_DEBUG` | false |
| `realtime_liveFeedPaused` | `LIVE_FEED_PAUSED` | false |

Feeds refresh on a schedule aligned to the clock: with the defaults, at 10 and 40 seconds past each minute. An unchanged card isn't written to, apart from every `realtime_liveFeedTimestampRefreshSeconds`, when it's written again with its data and a new `timestamp`. That also repairs a card whose data was overwritten by a browser poll at the same moment as an update.

If writing to a card fails but the card is still open, it's tried again on the next refresh with everything resent, and a finished feed is kept until every open card has been completed. A refused write (for example, not authorised) drops the card from the feed.

Each feed's refresh has `realtime_liveFeedRefreshTimeoutSeconds` to finish. After that it counts as failed and the other feeds carry on, so a data source that hangs can't stop every live card updating. A refresh that finishes after timing out writes nothing. That feed isn't refreshed again until its data source answers, or for at most six times the limit (2 minutes by default) in case the query never returns.

Setting `realtime_liveFeedPaused` to true stops all refreshing from the next cycle, without a restart: nothing is fetched, written or stopped, and open cards keep what they last showed. Set it back to false to resume.

The viewer timeout is for cleaning up cards whose tab was closed (closing a tab doesn't close the card). Keep it long: browsers slow down or pause polling in background tabs, and people leave live cards open in the background.

Feeds are held in memory, so this assumes one Tymly instance per database.

## Seeing what it's doing

- `GET /live-feed/status`, for users with the **Realtime Admin** role (`realtime_admin`): settings, feeds and their viewers, last run, last error.
- `realtime.live_feed_event`: notable events (subscribed, re-subscribed, changed, finished, dropped, stopped, failed), with the user's id and email, kept for `realtime_liveFeedEventRetentionDays`.
- `realtime_liveFeedDebug` set to true logs one line per feed per refresh.

## Testing

```bash
npm test
npm run lint
```
