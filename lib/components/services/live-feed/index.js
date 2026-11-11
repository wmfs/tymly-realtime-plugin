// Shared live feeds for cards that show live data (e.g. a list of open jobs, one job's details).
//
// Instead of every open card running its own refresh loop against the data source, cards
// subscribe to a feed (see the subscribeToLiveFeed state resource). Each feed - one per feed name
// and input, e.g. one per job being viewed - is refreshed once per cycle for all of its viewers:
//
//   - the feed's fetch function (a blueprint function) runs once and gets the shared data
//   - an optional present function (a blueprint function) runs per viewer, for anything that
//     depends on who is looking (e.g. hiding sensitive details)
//   - each viewer's execution is only written when its data has changed for that viewer.
//     Otherwise it's left alone (a heartbeat rewrites the whole execution row), apart from moving
//     its timestamp on every few minutes, so a card that has stopped refreshing can be spotted
//   - when the fetch says the feed has finished (e.g. the job has closed), each viewer gets
//     the full data as its last heartbeat, which completes its execution: the browser stops
//     polling and keeps showing the final data, and the feed is dropped
//
// Feeds refresh on a fixed schedule aligned to the clock (by default every 30 seconds, at 10 and
// 40 seconds past the minute, so one refresh lands just after a once-a-minute data load).
// A card can set its own intervalSeconds in the subscribeToLiveFeed ResourceConfig, otherwise the
// realtime_liveFeedIntervalSeconds registry key applies. The offset is shared by all feeds.
//
// A feed with no viewers left is dropped. Viewers are dropped when their execution stops, or
// stopped when nobody has polled them for a while (e.g. the tab was closed). Feeds are held in
// memory by the Tymly instance that started them, which assumes one Tymly instance per
// database: a second instance would re-subscribe the same cards on boot and refresh them too.
//
// Because they're in memory, on boot the service re-subscribes any cards that were still open
// (running executions whose context has a liveFeedSubscription, written by subscribeToLiveFeed).
// Those that are no longer being viewed are cleaned up on the first cycle.
//
// Settings are registry keys (realtime-blueprint/registry-keys), read from tymly_registryKey at the
// start of every cycle, so changing them in the database takes effect without a restart. On first
// boot each key is created from its environment variable if set, otherwise its default:
//
//   realtime_liveFeedIntervalSeconds          LIVE_FEED_INTERVAL_SECONDS           30
//   realtime_liveFeedOffsetSeconds            LIVE_FEED_OFFSET_SECONDS             10
//   realtime_liveFeedViewerTimeoutSeconds     LIVE_FEED_VIEWER_TIMEOUT_SECONDS     180
//   realtime_liveFeedTimestampRefreshSeconds  LIVE_FEED_TIMESTAMP_REFRESH_SECONDS  300 (0: every refresh)
//   realtime_liveFeedEventRetentionDays       LIVE_FEED_EVENT_RETENTION_DAYS       30
//   realtime_liveFeedStatusRole               LIVE_FEED_STATUS_ROLE                realtime_admin (template role in this plugin)
//   realtime_liveFeedDebug                    LIVE_FEED_DEBUG                      false
//   realtime_liveFeedPaused                   LIVE_FEED_PAUSED                     false (true stops all refreshing, see runCycle)
//
// Debugging:
//   - GET /live-feed/status (users with the status role) shows the settings, feeds and viewers
//   - notable events are written to realtime.live_feed_event (realtime_liveFeedEvent model), deleted
//     after the retention period (checked on boot and once a day)
//   - debug true logs one line per feed per cycle

const crypto = require('crypto')

// property on the service: registry key, environment variable, default, type, smallest allowed
const SETTINGS = {
  intervalSeconds: { key: 'realtime_liveFeedIntervalSeconds', env: 'LIVE_FEED_INTERVAL_SECONDS', default: 30, type: 'number', min: 1 },
  offsetSeconds: { key: 'realtime_liveFeedOffsetSeconds', env: 'LIVE_FEED_OFFSET_SECONDS', default: 10, type: 'number', min: 0 },
  viewerTimeoutSeconds: { key: 'realtime_liveFeedViewerTimeoutSeconds', env: 'LIVE_FEED_VIEWER_TIMEOUT_SECONDS', default: 180, type: 'number', min: 1 },
  timestampRefreshSeconds: { key: 'realtime_liveFeedTimestampRefreshSeconds', env: 'LIVE_FEED_TIMESTAMP_REFRESH_SECONDS', default: 300, type: 'number', min: 0 },
  eventRetentionDays: { key: 'realtime_liveFeedEventRetentionDays', env: 'LIVE_FEED_EVENT_RETENTION_DAYS', default: 30, type: 'number', min: 1 },
  statusRole: { key: 'realtime_liveFeedStatusRole', env: 'LIVE_FEED_STATUS_ROLE', default: 'realtime_admin', type: 'string' },
  debug: { key: 'realtime_liveFeedDebug', env: 'LIVE_FEED_DEBUG', default: false, type: 'boolean' },
  paused: { key: 'realtime_liveFeedPaused', env: 'LIVE_FEED_PAUSED', default: false, type: 'boolean' }
}

const ONE_DAY_MS = 24 * 60 * 60 * 1000

const EVENTS = {
  SUBSCRIBED: 'SUBSCRIBED',
  RESUBSCRIBED: 'RESUBSCRIBED',
  CHANGED: 'CHANGED',
  FINISHED: 'FINISHED',
  DROPPED: 'DROPPED',
  STOPPED: 'STOPPED',
  FAILED: 'FAILED'
}

// left out when deciding whether a viewer's data has changed, they change on every fetch
const VOLATILE_KEYS = ['timestamp', 'lastQueried', 'generated']

class LiveFeedService {
  constructor () {
    this.feeds = new Map()
    this.timer = null
    this.running = false
    this.shuttingDown = false

    // environment variables or defaults until the registry has been read
    for (const [name, setting] of Object.entries(SETTINGS)) {
      this[name] = parseSetting(setting, process.env[setting.env])
    }

    this.eventModel = null
    this.registryKeyModel = null

    // user id -> email, looked up when a user subscribes, for the status and the event log
    this.emails = new Map()
    this.emailLookups = new Map()
    this.lastCleanupAt = null

    // replaceable in tests
    this.now = () => Date.now()
  }

  async boot (options) {
    this.services = options.bootedServices
    this.env = { bootedServices: options.bootedServices }
    this.statebox = this.services.statebox
    this.functions = this.services.functions

    const { storage } = this.services
    const models = (storage && storage.models) || {}
    this.eventModel = models.realtime_liveFeedEvent || null
    this.registryKeyModel = models.tymly_registryKey || null
    this.executionModel = models.tymly_execution || null

    await this.loadSettings()

    options.messages.info(`Live feeds refresh every ${this.intervalSeconds}s, ${this.offsetSeconds}s past the interval`)

    this.addStatusRoute()
    this.scheduleNextCycle()
    this.cleanUpEventsIfDue()

    // doesn't hold up boot, the first cycle is at least a few seconds away
    this.resubscribeRunningViewers()
      .then(count => { if (count > 0) console.log(`Live feeds: re-subscribed ${count} open card(s) after boot`) })
      .catch(err => console.log('Live feeds: could not re-subscribe open cards after boot', err))
  }

  // Reads the settings from tymly_registryKey. A missing or invalid value keeps the current one.
  async loadSettings () {
    if (!this.registryKeyModel) return

    await Promise.all(Object.entries(SETTINGS).map(async ([name, setting]) => {
      try {
        const doc = await this.registryKeyModel.findById(setting.key)
        const raw = doc && doc.value ? doc.value.value : undefined
        if (raw === undefined || raw === null || raw === '') return

        const value = parseSetting(setting, raw, null)

        if (value === null) {
          console.log(`Live feeds: ignoring invalid ${setting.key} value '${raw}'`)
        } else if (value !== this[name]) {
          if (this[name] !== undefined) console.log(`Live feeds: ${setting.key} changed from ${this[name]} to ${value}`)
          this[name] = value
        }
      } catch (err) {
        console.log(`Live feeds: could not read ${setting.key}`, err.message)
      }
    }))
  }

  // Deletes old events on boot and then once a day
  cleanUpEventsIfDue () {
    if (this.lastCleanupAt !== null && this.now() - this.lastCleanupAt < ONE_DAY_MS) return
    this.lastCleanupAt = this.now()

    this.deleteOldEvents()
      .catch(err => console.log('Live feeds: could not delete old events', err))
  }

  // After a restart, picks up cards that are still open, so they carry on refreshing.
  // Returns how many were re-subscribed.
  async resubscribeRunningViewers () {
    const { statebox, storage } = this.services || {}
    const executionModel = storage && storage.models && storage.models.tymly_execution

    if (!executionModel || !statebox || typeof statebox.findStates !== 'function') return 0

    const stateMachineNames = [...new Set(
      statebox.findStates({ resourceToFind: 'module:subscribeToLiveFeed' })
        .map(state => state.stateMachine && state.stateMachine.name)
        .filter(Boolean)
    )]

    let count = 0

    for (const stateMachineName of stateMachineNames) {
      const executions = await executionModel.find({
        where: {
          stateMachineName: { equals: stateMachineName },
          status: { equals: 'RUNNING' }
        }
      })

      for (const execution of executions) {
        const subscription = execution.ctx && execution.ctx.liveFeedSubscription
        if (!subscription) continue

        this.subscribe({
          ...subscription,
          executionName: execution.executionName,
          userId: execution.executionOptions && execution.executionOptions.userId
        }, EVENTS.RESUBSCRIBED)

        count++
      }
    }

    return count
  }

  // Adds a viewer (a waiting execution) to a feed, creating the feed if it's new.
  // Returns the feed's key.
  subscribe ({ feed, input = {}, fetchFunction, presentFunction, intervalSeconds, executionName, userId, viewerTimeoutSeconds }, event = EVENTS.SUBSCRIBED) {
    if (!feed) throw new Error('Live feed subscription needs a feed name')
    if (!fetchFunction) throw new Error(`Live feed '${feed}' needs a fetchFunction`)
    if (!executionName) throw new Error(`Live feed '${feed}' subscription needs an executionName`)

    const key = feedKey(feed, input)
    let entry = this.feeds.get(key)

    if (!entry) {
      entry = {
        key,
        feed,
        input: clone(input),
        fetchFunction,
        presentFunction,
        // optional override from the state's ResourceConfig, otherwise the current setting
        intervalSeconds: validInterval(intervalSeconds),
        nextDueAt: null,
        viewers: new Map(),
        stats: { runs: 0, lastRunAt: null, lastDurationMs: null, lastError: null, lastErrorAt: null },
        // the error it's currently failing with, so a feed that keeps failing records it once
        failing: null
      }
      entry.nextDueAt = this.nextDueAt(entry)
      this.feeds.set(key, entry)

      // a feed due sooner than the current timer needs the timer brought forward
      if (this.timer && !this.running) this.scheduleNextCycle()
    }

    entry.viewers.set(executionName, {
      executionName,
      userId,
      // optional override from the state's ResourceConfig, otherwise the current setting
      timeoutSeconds: viewerTimeoutSeconds || null,
      subscribedAt: this.now(),
      lastChangedAt: null,
      lastUpdatedAt: null,
      lastHash: null,
      presentFailing: null
    })

    // records the event once the user's email is known, without holding up the subscription
    this.lookUpEmail(userId)
      .then(() => this.recordEvent(event, entry, { executionName, userId }))

    return key
  }

  // Everything about the current feeds, for the status endpoint
  status () {
    const iso = ms => ms ? new Date(ms).toISOString() : null

    const settings = {}
    for (const name of Object.keys(SETTINGS)) settings[name] = this[name]

    return {
      settings,
      intervalSeconds: this.intervalSeconds,
      offsetSeconds: this.offsetSeconds,
      paused: this.paused,
      cycleRunning: this.running,
      nextCycleInSeconds: this.shuttingDown ? null : Math.round(this.msUntilNextWake() / 1000),
      feeds: [...this.feeds.values()].map(entry => ({
        key: entry.key,
        feed: entry.feed,
        input: entry.input,
        fetchFunction: entry.fetchFunction,
        presentFunction: entry.presentFunction || null,
        intervalSeconds: this.feedInterval(entry),
        intervalFrom: entry.intervalSeconds ? 'resourceConfig' : 'registry',
        nextRunAt: iso(entry.nextDueAt),
        runs: entry.stats.runs,
        lastRunAt: iso(entry.stats.lastRunAt),
        lastDurationMs: entry.stats.lastDurationMs,
        lastError: entry.stats.lastError,
        lastErrorAt: iso(entry.stats.lastErrorAt),
        viewers: [...entry.viewers.values()].map(viewer => ({
          executionName: viewer.executionName,
          userId: viewer.userId || null,
          userEmail: this.emails.get(viewer.userId) || null,
          subscribedAt: iso(viewer.subscribedAt),
          lastChangedAt: iso(viewer.lastChangedAt),
          lastUpdatedAt: iso(viewer.lastUpdatedAt)
        }))
      }))
    }
  }

  // GET /live-feed/status, for logged-in users with the status role
  addStatusRoute () {
    const { server, jwtAuth } = this.services || {}
    if (!server || !server.app || !server.app.jwtCheck || !jwtAuth) return

    const router = async app => {
      app.get('/status', { preValidation: [app.jwtCheck] }, async (request, reply) => {
        const userId = jwtAuth.extractUserIdFromRequest(request)

        if (!(await this.canSeeStatus(userId))) {
          return reply.code(403).send({ message: `Needs the ${this.statusRole} role` })
        }

        return reply.send(this.status())
      })
    }

    server.app.register(router, { prefix: '/live-feed' })
  }

  async canSeeStatus (userId) {
    const { rbac } = this.services || {}
    if (!userId || !rbac || typeof rbac.listUserRoles !== 'function') return false

    const roles = await rbac.listUserRoles(userId)
    return Array.isArray(roles) && roles.includes(this.statusRole)
  }

  // Writes a row to realtime.live_feed_event. Never throws: the feed carries on if it can't.
  async recordEvent (event, entry, { executionName = null, userId = null, detail = null } = {}) {
    if (!this.eventModel) return

    try {
      await this.eventModel.create({
        event,
        feedKey: entry ? entry.key : null,
        feed: entry ? entry.feed : null,
        executionName,
        userId,
        userEmail: (userId && this.emails.get(userId)) || null,
        detail: detail === null ? null : String(detail).slice(0, 2000)
      })
    } catch (err) {
      console.log(`Live feeds: could not record ${event} event`, err.message)
    }
  }

  // Finds a user's email with the userInfo service (from the Auth0 plugin), which caches it.
  // Never throws: without it, the status and events just show the user id.
  lookUpEmail (userId) {
    if (!userId) return Promise.resolve(null)
    if (this.emails.has(userId)) return Promise.resolve(this.emails.get(userId))

    // two tabs subscribing at once share one lookup
    if (!this.emailLookups.has(userId)) {
      const lookup = this.fetchEmail(userId)
        .finally(() => this.emailLookups.delete(userId))
      this.emailLookups.set(userId, lookup)
    }

    return this.emailLookups.get(userId)
  }

  async fetchEmail (userId) {
    const { userInfo } = this.services || {}
    if (!userInfo || typeof userInfo.emailFromUserId !== 'function') return null

    try {
      const email = await userInfo.emailFromUserId(userId)
      if (email) this.emails.set(userId, email)
      return email || null
    } catch (err) {
      console.log(`Live feeds: could not look up the email for ${userId}: ${err.message}`)
      return null
    }
  }

  async deleteOldEvents () {
    const { storage } = this.services || {}
    const client = storage && storage.client
    if (!this.eventModel || !client) return

    await client.query(
      'DELETE FROM realtime.live_feed_event WHERE _created < now() - ($1::integer * interval \'1 day\')',
      [Math.round(this.eventRetentionDays)]
    )
  }

  // A summary of the current feeds, for logging and tests
  listFeeds () {
    return [...this.feeds.values()].map(entry => ({
      key: entry.key,
      feed: entry.feed,
      viewers: [...entry.viewers.keys()]
    }))
  }

  // Milliseconds until the next default-interval cycle, aligned to the clock
  msUntilNextCycle (now = this.now()) {
    return msUntilNext(now, this.intervalSeconds, this.offsetSeconds)
  }

  feedInterval (entry) {
    return entry.intervalSeconds || this.intervalSeconds
  }

  // When a feed next needs refreshing, aligned to the clock on its own interval
  nextDueAt (entry, now = this.now()) {
    return now + msUntilNext(now, this.feedInterval(entry), this.offsetSeconds)
  }

  // The timer wakes for whichever comes first: a feed that's due, or the default-interval cycle
  // (which also re-reads the settings and tidies up)
  msUntilNextWake (now = this.now()) {
    let wait = this.msUntilNextCycle(now)

    for (const entry of this.feeds.values()) {
      if (entry.nextDueAt !== null) wait = Math.min(wait, entry.nextDueAt - now)
    }

    return Math.max(wait, 0)
  }

  scheduleNextCycle () {
    if (this.shuttingDown) return
    if (this.timer) clearTimeout(this.timer)

    this.timer = setTimeout(() => this.runScheduledCycle(), this.msUntilNextWake())

    // don't keep the process alive just for the live feeds
    if (this.timer.unref) this.timer.unref()
  }

  async runScheduledCycle () {
    this.timer = null

    try {
      await this.runCycle({ dueOnly: true })
    } catch (err) {
      console.log('Live feed cycle failed', err)
    } finally {
      this.scheduleNextCycle()
    }
  }

  // Refreshes the feeds that are due (or all of them, when dueOnly is false). Feeds run one after
  // another, and a cycle is skipped if the previous one is still running.
  async runCycle ({ dueOnly = false } = {}) {
    if (this.running) {
      console.log('Live feed cycle skipped, the previous one is still running')
      return
    }

    this.running = true

    try {
      // settings changed in the database apply from this cycle (interval and offset from the next)
      await this.loadSettings()
      this.cleanUpEventsIfDue()

      // Paused (realtime_liveFeedPaused): nothing is fetched, written or stopped. Cards keep what
      // they last showed, and new cards still subscribe. Refreshing resumes on the first cycle
      // after it's set back to false, because the settings above are still read every cycle.
      if (this.paused) {
        if (this.debug) console.log(`Live feeds paused, ${this.feeds.size} feed(s) waiting`)
        return
      }

      for (const entry of [...this.feeds.values()]) {
        if (this.shuttingDown) break

        const started = this.now()

        // a timer can fire a little early, so allow a second
        if (dueOnly && entry.nextDueAt !== null && entry.nextDueAt > started + 1000) continue

        try {
          await this.runFeed(entry)
          entry.failing = null
        } catch (err) {
          // one failed feed must not stop the others, its viewers keep their last data
          entry.stats.lastError = err.message
          entry.stats.lastErrorAt = this.now()

          // logged and recorded once while it keeps failing the same way, not every cycle
          if (entry.failing !== err.message) {
            console.log(`Live feed '${entry.key}' failed`, err)
            await this.recordEvent(EVENTS.FAILED, entry, { detail: err.message })
            entry.failing = err.message
          }
        } finally {
          entry.stats.runs++
          entry.stats.lastRunAt = started
          entry.stats.lastDurationMs = this.now() - started
          // from when it started, so a slow run doesn't push the schedule back
          entry.nextDueAt = this.nextDueAt(entry, Math.max(started, entry.nextDueAt || 0))
        }
      }
    } finally {
      this.running = false
    }
  }

  async runFeed (entry) {
    await this.pruneViewers(entry)

    if (entry.viewers.size === 0) {
      this.feeds.delete(entry.key)
      if (this.debug) console.log(`Live feed '${entry.key}': no viewers left, dropped`)
      return
    }

    const fetched = await this.callFunction(entry.fetchFunction, { event: clone(entry.input) })
    const { data = {}, finished = false } = fetched || {}
    const timestamp = this.timestamp()
    let changed = 0
    let written = 0

    for (const viewer of [...entry.viewers.values()]) {
      let viewerData

      try {
        viewerData = entry.presentFunction
          ? await this.callFunction(entry.presentFunction, { event: clone(data), context: { userId: viewer.userId } })
          : data
      } catch (err) {
        if (viewer.presentFailing !== err.message) {
          console.log(`Live feed '${entry.key}' failed to present data for ${viewer.executionName}`, err)
          await this.recordEvent(EVENTS.FAILED, entry, { executionName: viewer.executionName, userId: viewer.userId, detail: `present: ${err.message}` })
          viewer.presentFailing = err.message
        }

        // the feed is about to be dropped, so complete this card too, showing what it already has
        if (finished) await this.completeViewer(entry, viewer, timestamp)
        continue
      }

      viewer.presentFailing = null

      const hash = hashOf(viewerData)
      const isChange = hash !== viewer.lastHash

      // nothing new: leave the execution alone unless its timestamp is due to move on
      if (!isChange && !finished && !this.timestampDue(viewer)) continue

      // the last update always carries the full data, it's what the card is left showing
      const update = isChange || finished
        ? { ...viewerData, timestamp }
        : { timestamp }

      // the last heartbeat completes the viewer's execution
      const send = finished ? 'sendTaskLastHeartbeat' : 'sendTaskHeartbeat'

      try {
        const result = await this.statebox[send](
          viewer.executionName,
          { requiredHumanInput: { data: update } },
          { userId: viewer.userId }
        )

        if (result && result.status === 'FAILED') {
          throw new Error(`${result.errorCode} ${result.errorMessage}`)
        }

        viewer.lastHash = hash
        viewer.lastUpdatedAt = this.now()
        written++

        if (isChange) {
          changed++
          viewer.lastChangedAt = viewer.lastUpdatedAt
          await this.recordEvent(EVENTS.CHANGED, entry, { executionName: viewer.executionName, userId: viewer.userId })
        }
      } catch (err) {
        console.log(`Live feed '${entry.key}' dropped ${viewer.executionName}, it could not be updated: ${err.message}`)
        entry.viewers.delete(viewer.executionName)
        await this.recordEvent(EVENTS.DROPPED, entry, { executionName: viewer.executionName, userId: viewer.userId, detail: `could not be updated: ${err.message}` })
      }
    }

    if (this.debug) {
      console.log(`Live feed '${entry.key}': ${entry.viewers.size} viewer(s), ${changed} changed, ${written} written${finished ? ', finished' : ''}`)
    }

    // e.g. the job has closed, so there's nothing more to refresh. The viewers' executions
    // were completed by their last heartbeat above.
    if (finished) {
      this.feeds.delete(entry.key)
      await this.recordEvent(EVENTS.FINISHED, entry, { detail: `completed ${entry.viewers.size} viewer(s)` })
    }
  }

  // Whether an unchanged viewer's timestamp should be moved on this refresh
  timestampDue (viewer) {
    if (!this.timestampRefreshSeconds) return true
    if (!viewer.lastUpdatedAt) return true
    return this.now() - viewer.lastUpdatedAt >= this.timestampRefreshSeconds * 1000
  }

  // Completes a viewer's execution with only a new timestamp, so it keeps the data it has
  async completeViewer (entry, viewer, timestamp) {
    try {
      await this.statebox.sendTaskLastHeartbeat(
        viewer.executionName,
        { requiredHumanInput: { data: { timestamp } } },
        { userId: viewer.userId }
      )
    } catch (err) {
      console.log(`Live feed '${entry.key}' could not complete ${viewer.executionName}: ${err.message}`)
    }
  }

  async pruneViewers (entry) {
    for (const viewer of [...entry.viewers.values()]) {
      let execution

      try {
        execution = await this.findExecution(viewer.executionName)
      } catch (err) {
        // couldn't tell, so keep the viewer and try again next cycle
        console.log(`Live feed '${entry.key}' could not look up ${viewer.executionName}: ${err.message}`)
        continue
      }

      // Not found is normal: leaving a card stops and archives its execution, and with
      // clearTaskAfterCompleted the row is deleted
      if (!execution || execution.status !== 'RUNNING') {
        entry.viewers.delete(viewer.executionName)
        await this.recordEvent(EVENTS.DROPPED, entry, {
          executionName: viewer.executionName,
          userId: viewer.userId,
          detail: execution ? `execution ${execution.status}` : 'execution not found (card closed)'
        })
        continue
      }

      // never polled at all counts from when it subscribed
      const lastSeen = execution.lastDescribed ? new Date(execution.lastDescribed).getTime() : viewer.subscribedAt

      const timeoutSeconds = viewer.timeoutSeconds || this.viewerTimeoutSeconds

      if (this.now() - lastSeen > timeoutSeconds * 1000) {
        // nobody is looking at it any more (e.g. the tab was closed), so stop it
        try {
          await this.statebox.stopExecution(
            'Execution stopped internally, no longer being viewed',
            'STOPPED',
            viewer.executionName,
            {
              userId: viewer.userId,
              action: 'stopExecution',
              stateMachineName: execution.stateMachineName
            }
          )
        } catch (err) {
          console.log(`Live feed '${entry.key}' could not stop ${viewer.executionName}`, err)
        }

        entry.viewers.delete(viewer.executionName)
        await this.recordEvent(EVENTS.STOPPED, entry, {
          executionName: viewer.executionName,
          userId: viewer.userId,
          detail: `not polled since ${new Date(lastSeen).toISOString()}`
        })
      }
    }
  }

  // Reads an execution's status and lastDescribed, or null if it no longer exists. Reads the row
  // directly because statebox's describeExecution saves the whole execution back each time, and
  // throws when it's missing.
  async findExecution (executionName) {
    if (this.executionModel) {
      // only what's needed, not the whole context
      const [execution] = await this.executionModel.find({
        where: { executionName: { equals: executionName } },
        fields: ['executionName', 'status', 'lastDescribed', 'stateMachineName']
      })
      return execution || null
    }

    try {
      return await this.statebox.describeExecution(executionName, { updateLastDescribed: false })
    } catch (err) {
      if (isNotFound(err)) return null
      throw err
    }
  }

  // Calls a blueprint function the way Tymly's function: resources do, by argument name
  async callFunction (functionId, { event, context = {} }) {
    const info = this.functions && this.functions.functions[functionId]

    if (!info) {
      throw new Error(`Live feed function '${functionId}' not found`)
    }

    const hasCallback = info.args.includes('callback')

    const args = info.args
      .filter(arg => arg !== 'callback')
      .map(arg => {
        if (arg === 'event') return event
        if (arg === 'env') return this.env
        if (arg === 'context') return context
        return event[arg]
      })

    if (hasCallback) {
      return new Promise((resolve, reject) => {
        info.func(...args, (err, data) => err ? reject(err) : resolve(data))
      })
    }

    return info.func(...args)
  }

  timestamp () {
    const { timestamp } = this.services || {}
    return timestamp ? timestamp.now() : new Date()
  }

  shutdown () {
    this.shuttingDown = true

    if (this.timer) {
      clearTimeout(this.timer)
      this.timer = null
    }

    this.feeds.clear()
    this.emails.clear()
  }
}

function feedKey (feed, input) {
  return `${feed}:${stableStringify(input)}`
}

function hashOf (data) {
  const comparable = { ...data }
  for (const key of VOLATILE_KEYS) delete comparable[key]

  return crypto.createHash('sha1').update(stableStringify(comparable)).digest('hex')
}

// JSON with object keys sorted, so the same data always gives the same string
function stableStringify (value) {
  return JSON.stringify(canonical(value))
}

function canonical (value) {
  if (value && typeof value.toJSON === 'function') return canonical(value.toJSON())
  if (Array.isArray(value)) return value.map(canonical)

  if (value && typeof value === 'object') {
    const sorted = {}
    for (const key of Object.keys(value).sort()) sorted[key] = canonical(value[key])
    return sorted
  }

  return value
}

function clone (value) {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value))
}

function isNotFound (err) {
  return !!err && (
    (err.output && err.output.statusCode === 400 && /Unable to describe/.test(err.message)) ||
    /Unable to describe execution/.test(err.message || '')
  )
}

// Milliseconds from now until the next point that's offsetSeconds past a whole intervalSeconds
function msUntilNext (now, intervalSeconds, offsetSeconds) {
  const interval = intervalSeconds * 1000
  const offset = (offsetSeconds * 1000) % interval
  let wait = offset - (now % interval)
  if (wait <= 0) wait += interval
  return wait
}

function validInterval (intervalSeconds) {
  const n = Number(intervalSeconds)
  return Number.isFinite(n) && n >= 1 ? n : null
}

// Turns a registry or environment value into a setting's type. Returns the fallback (by default
// the setting's default) when the value is missing or invalid.
function parseSetting (setting, raw, fallback = setting.default) {
  if (raw === undefined || raw === null || raw === '') return fallback

  if (setting.type === 'number') {
    const n = Number(raw)
    return Number.isFinite(n) && n >= setting.min ? n : fallback
  }

  if (setting.type === 'boolean') {
    if (raw === true || raw === 'true') return true
    if (raw === false || raw === 'false') return false
    return fallback
  }

  return String(raw)
}

module.exports = {
  serviceClass: LiveFeedService,
  bootAfter: ['statebox', 'storage', 'functions', 'timestamp', 'server', 'jwtAuth'],
  // exported for tests
  EVENTS,
  SETTINGS,
  feedKey,
  hashOf,
  stableStringify
}
