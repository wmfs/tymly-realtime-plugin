/* eslint-env mocha */

// unit tests for the live feed service, no database needed

const expect = require('chai').expect
const { serviceClass: LiveFeedService, feedKey, hashOf, SETTINGS } = require('../lib/components/services/live-feed')

const T0 = Date.parse('2026-11-10T12:00:05Z')

// a fake statebox that records heartbeats and stops
function fakeStatebox (executions = {}) {
  return {
    heartbeats: [],
    stopped: [],
    async describeExecution (name) {
      return executions[name] || null
    },
    async sendTaskHeartbeat (name, result, options) {
      this.heartbeats.push({ name, data: result.requiredHumanInput.data, userId: options.userId, last: false })
      return { status: 'RUNNING' }
    },
    async sendTaskLastHeartbeat (name, result, options) {
      this.heartbeats.push({ name, data: result.requiredHumanInput.data, userId: options.userId, last: true })
      return { status: 'RUNNING' }
    },
    async stopExecution (cause, error, name) {
      this.stopped.push(name)
    }
  }
}

function running (lastDescribed = T0) {
  return { status: 'RUNNING', lastDescribed: new Date(lastDescribed).toISOString(), stateMachineName: 'test_card_1_0' }
}

// fake blueprint functions, registered the way the functions service holds them
function fakeFunctions (defs) {
  const functions = {}
  for (const [id, { func, args }] of Object.entries(defs)) functions[id] = { func, args }
  return { functions }
}

function makeService ({ executions, functions }) {
  const service = new LiveFeedService()
  service.now = () => T0
  service.statebox = fakeStatebox(executions)
  service.functions = fakeFunctions(functions)
  service.env = { bootedServices: {} }
  service.services = {}
  return service
}

describe('Live feed', () => {
  describe('scheduling', () => {
    const service = new LiveFeedService()
    service.intervalSeconds = 60
    service.offsetSeconds = 10

    it('waits until 10 seconds past the next minute', () => {
      expect(service.msUntilNextCycle(Date.parse('2026-11-10T12:00:05Z'))).to.eql(5000)
      expect(service.msUntilNextCycle(Date.parse('2026-11-10T12:00:30Z'))).to.eql(40000)
    })

    it('waits a whole interval when exactly on the offset', () => {
      expect(service.msUntilNextCycle(Date.parse('2026-11-10T12:00:10Z'))).to.eql(60000)
    })
  })

  describe('per-feed interval', () => {
    let fetched, service, now

    beforeEach(() => {
      fetched = []
      now = T0 // 12:00:05
      service = makeService({
        executions: { a: running(), b: running() },
        functions: {
          test_fetch: { args: ['event'], func: async event => { fetched.push(event.which); return { data: { n: fetched.length } } } }
        }
      })
      service.now = () => now
      service.intervalSeconds = 60
      service.offsetSeconds = 10
      service.viewerTimeoutSeconds = 100000
    })

    it('uses the interval from the ResourceConfig, otherwise the setting', () => {
      service.subscribe({ feed: 'fast', input: { which: 'fast' }, fetchFunction: 'test_fetch', intervalSeconds: 15, executionName: 'a' })
      service.subscribe({ feed: 'slow', input: { which: 'slow' }, fetchFunction: 'test_fetch', executionName: 'b' })

      const [fast, slow] = service.status().feeds
      expect(fast.intervalSeconds).to.eql(15)
      expect(fast.intervalFrom).to.eql('resourceConfig')
      expect(fast.nextRunAt).to.eql('2026-11-10T12:00:10.000Z')
      expect(slow.intervalSeconds).to.eql(60)
      expect(slow.intervalFrom).to.eql('registry')
      expect(slow.nextRunAt).to.eql('2026-11-10T12:00:10.000Z')
    })

    it('ignores an invalid override', () => {
      service.subscribe({ feed: 'x', fetchFunction: 'test_fetch', intervalSeconds: 'soon', executionName: 'a' })
      expect(service.status().feeds[0].intervalSeconds).to.eql(60)
    })

    it('only refreshes the feeds that are due', async () => {
      service.subscribe({ feed: 'fast', input: { which: 'fast' }, fetchFunction: 'test_fetch', intervalSeconds: 15, executionName: 'a' })
      service.subscribe({ feed: 'slow', input: { which: 'slow' }, fetchFunction: 'test_fetch', executionName: 'b' })

      const at = async time => {
        now = Date.parse(time)
        await service.runCycle({ dueOnly: true })
      }

      await at('2026-11-10T12:00:10Z') // both due
      await at('2026-11-10T12:00:25Z') // fast only
      await at('2026-11-10T12:00:40Z') // fast only
      await at('2026-11-10T12:00:55Z') // fast only
      await at('2026-11-10T12:01:10Z') // both

      expect(fetched).to.eql(['fast', 'slow', 'fast', 'fast', 'fast', 'fast', 'slow'])
    })

    it('wakes for whichever feed is due first', () => {
      expect(service.msUntilNextWake()).to.eql(5000)
      now = Date.parse('2026-11-10T12:00:11Z')
      service.subscribe({ feed: 'fast', fetchFunction: 'test_fetch', intervalSeconds: 15, executionName: 'a' })
      expect(service.msUntilNextWake()).to.eql(14000) // 12:00:25, before the minute cycle at 12:01:10
    })

    it('refreshes every feed when not limited to the due ones', async () => {
      service.subscribe({ feed: 'fast', input: { which: 'fast' }, fetchFunction: 'test_fetch', intervalSeconds: 15, executionName: 'a' })
      service.subscribe({ feed: 'slow', input: { which: 'slow' }, fetchFunction: 'test_fetch', executionName: 'b' })
      await service.runCycle()
      expect(fetched).to.eql(['fast', 'slow'])
    })
  })

  describe('subscribing', () => {
    it('shares one feed between viewers of the same input, whatever the key order', () => {
      const service = makeService({ functions: {} })
      service.subscribe({ feed: 'summary', input: { job: { id: 1, region: 'north' } }, fetchFunction: 'f', executionName: 'a' })
      service.subscribe({ feed: 'summary', input: { job: { region: 'north', id: 1 } }, fetchFunction: 'f', executionName: 'b' })
      service.subscribe({ feed: 'summary', input: { job: { id: 2, region: 'north' } }, fetchFunction: 'f', executionName: 'c' })

      const feeds = service.listFeeds()
      expect(feeds.length).to.eql(2)
      expect(feeds[0].viewers).to.eql(['a', 'b'])
      expect(feeds[1].viewers).to.eql(['c'])
    })

    it('needs a feed name, fetch function and execution name', () => {
      const service = makeService({ functions: {} })
      expect(() => service.subscribe({ fetchFunction: 'f', executionName: 'a' })).to.throw()
      expect(() => service.subscribe({ feed: 'x', executionName: 'a' })).to.throw()
      expect(() => service.subscribe({ feed: 'x', fetchFunction: 'f' })).to.throw()
    })

    it('builds stable feed keys', () => {
      expect(feedKey('x', { b: 1, a: 2 })).to.eql(feedKey('x', { a: 2, b: 1 }))
    })
  })

  describe('refreshing', () => {
    let fetches, data, service

    beforeEach(() => {
      fetches = 0
      data = { items: [1, 2], lastQueried: 'q1' }

      service = makeService({
        executions: { a: running(), b: running() },
        functions: {
          test_fetch: { args: ['event', 'env'], func: async () => { fetches++; return { data: { ...data } } } },
          test_present: {
            args: ['event', 'env', 'context'],
            func: async (event, env, context) => ({ ...event, viewer: context.userId })
          }
        }
      })

      service.subscribe({ feed: 'live', input: {}, fetchFunction: 'test_fetch', presentFunction: 'test_present', executionName: 'a', userId: 'alice' })
      service.subscribe({ feed: 'live', input: {}, fetchFunction: 'test_fetch', presentFunction: 'test_present', executionName: 'b', userId: 'bob' })
    })

    it('fetches once for all viewers and presents for each one', async () => {
      await service.runCycle()

      expect(fetches).to.eql(1)
      expect(service.statebox.heartbeats.length).to.eql(2)

      const [a, b] = service.statebox.heartbeats
      expect(a.name).to.eql('a')
      expect(a.userId).to.eql('alice')
      expect(a.data.viewer).to.eql('alice')
      expect(a.data.items).to.eql([1, 2])
      expect(a.data.timestamp).to.not.eql(undefined)
      expect(b.data.viewer).to.eql('bob')
    })

    it('leaves an unchanged viewer alone, so its execution is not rewritten', async () => {
      service.timestampRefreshSeconds = 300
      await service.runCycle()
      data.lastQueried = 'q2' // volatile, doesn't count as a change
      await service.runCycle()

      expect(service.statebox.heartbeats.length).to.eql(2)
    })

    it('moves an unchanged viewer\'s timestamp on once the refresh time has passed', async () => {
      let now = T0
      service.now = () => now
      service.timestampRefreshSeconds = 300
      service.viewerTimeoutSeconds = 100000 // still being polled

      await service.runCycle()
      now += 299 * 1000
      await service.runCycle()
      expect(service.statebox.heartbeats.length).to.eql(2)

      now += 1000
      await service.runCycle()
      const third = service.statebox.heartbeats.slice(2)
      expect(third.length).to.eql(2)
      expect(Object.keys(third[0].data)).to.eql(['timestamp'])
    })

    it('sends only a timestamp every refresh when the timestamp refresh is 0', async () => {
      service.timestampRefreshSeconds = 0
      await service.runCycle()
      await service.runCycle()

      const second = service.statebox.heartbeats.slice(2)
      expect(second.length).to.eql(2)
      expect(Object.keys(second[0].data)).to.eql(['timestamp'])
    })

    it('still completes a viewer whose data could not be presented when the feed finishes', async () => {
      await service.runCycle()
      service.functions.functions.test_fetch.func = async () => ({ data: { ...data }, finished: true })
      service.functions.functions.test_present.func = async (event, env, context) => {
        if (context.userId === 'bob') throw new Error('roles unavailable')
        return { ...event, viewer: context.userId }
      }
      await service.runCycle()

      const last = service.statebox.heartbeats.slice(2)
      expect(last.map(h => [h.name, h.last])).to.eql([['a', true], ['b', true]])
      expect(Object.keys(last[1].data)).to.eql(['timestamp'])
      expect(service.listFeeds()).to.eql([])
    })

    it('sends the full data again when it changes', async () => {
      await service.runCycle()
      data.items = [1, 2, 3]
      await service.runCycle()

      const second = service.statebox.heartbeats.slice(2)
      expect(second[0].data.items).to.eql([1, 2, 3])
    })

    it('drops a viewer whose execution has stopped', async () => {
      service.statebox = fakeStatebox({ a: running(), b: { status: 'STOPPED' } })
      await service.runCycle()

      expect(service.statebox.heartbeats.map(h => h.name)).to.eql(['a'])
      expect(service.listFeeds()[0].viewers).to.eql(['a'])
    })

    it('drops a viewer whose execution has been deleted, without failing', async () => {
      service.statebox.describeExecution = async name => {
        if (name === 'a') throw new Error(`Unable to describe execution with name '${name}'`)
        return running()
      }
      await service.runCycle()
      expect(service.listFeeds()[0].viewers).to.eql(['b'])
    })

    it('keeps a viewer when its execution cannot be looked up, and tries again next cycle', async () => {
      service.statebox.describeExecution = async name => {
        if (name === 'a') throw new Error('connection reset')
        return running()
      }
      await service.runCycle()
      expect(service.listFeeds()[0].viewers).to.eql(['a', 'b'])
    })

    it('reads executions from the execution model when there is one, without writing them', async () => {
      const rows = { a: running(), b: null }
      const queries = []
      service.executionModel = {
        find: async query => {
          queries.push(query)
          const row = rows[query.where.executionName.equals]
          return row ? [row] : []
        }
      }
      service.statebox.describeExecution = async () => { throw new Error('should not be called') }
      await service.runCycle()
      expect(service.listFeeds()[0].viewers).to.eql(['a'])
      expect(queries[0].fields).to.not.include('ctx')
    })

    it('stops a viewer nobody has polled for longer than its timeout', async () => {
      service.statebox = fakeStatebox({ a: running(), b: running(T0 - 240000) })
      await service.runCycle()

      expect(service.statebox.stopped).to.eql(['b'])
      expect(service.listFeeds()[0].viewers).to.eql(['a'])
    })

    it('keeps a card in a background tab that has polled within the 3-minute default', async () => {
      service.statebox = fakeStatebox({ a: running(), b: running(T0 - 150000) })
      await service.runCycle()

      expect(service.statebox.stopped).to.eql([])
      expect(service.listFeeds()[0].viewers).to.eql(['a', 'b'])
    })

    it('drops the feed, without fetching, when no viewers are left', async () => {
      service.statebox = fakeStatebox({})
      await service.runCycle()

      expect(fetches).to.eql(0)
      expect(service.listFeeds()).to.eql([])
    })

    it('drops a viewer that cannot be updated', async () => {
      service.statebox.sendTaskHeartbeat = async function (name) {
        if (name === 'b') return { status: 'FAILED', errorCode: '401', errorMessage: 'not allowed' }
        this.heartbeats.push({ name })
        return { status: 'RUNNING' }
      }

      await service.runCycle()
      expect(service.listFeeds()[0].viewers).to.eql(['a'])
    })

    it('completes the viewers with a last heartbeat and drops the feed once the fetch says it has finished', async () => {
      service.functions.functions.test_fetch.func = async () => ({ data: { closed: true }, finished: true })
      await service.runCycle()

      const heartbeats = service.statebox.heartbeats
      expect(heartbeats.length).to.eql(2)
      expect(heartbeats.every(h => h.last)).to.eql(true)
      expect(heartbeats[0].data.closed).to.eql(true)
      expect(service.listFeeds()).to.eql([])
    })

    it('sends the full data as the last heartbeat even when nothing else changed', async () => {
      await service.runCycle()
      service.functions.functions.test_fetch.func = async () => ({ data: { ...data }, finished: true })
      await service.runCycle()

      const last = service.statebox.heartbeats.slice(2)
      expect(last.every(h => h.last)).to.eql(true)
      expect(last[0].data.items).to.eql([1, 2])
    })

    it('keeps going with other feeds when one fails', async () => {
      service.functions.functions.test_broken = { args: ['event'], func: async () => { throw new Error('database down') } }
      service.statebox = fakeStatebox({ a: running(), b: running(), c: running() })
      service.subscribe({ feed: 'broken', input: {}, fetchFunction: 'test_broken', executionName: 'c', userId: 'carol' })

      await service.runCycle()

      expect(fetches).to.eql(1)
      expect(service.statebox.heartbeats.map(h => h.name)).to.eql(['a', 'b'])
      // the failed feed and its viewer are kept for the next cycle
      expect(service.listFeeds().length).to.eql(2)
    })

    it('skips a cycle while the previous one is still running', async () => {
      service.running = true
      await service.runCycle()
      expect(fetches).to.eql(0)
    })
  })

  describe('refresh time limit', () => {
    function deferred () {
      const handle = {}
      handle.promise = new Promise(resolve => { handle.resolve = resolve })
      return handle
    }

    let service, hung, events, otherFetches

    beforeEach(() => {
      hung = deferred()
      otherFetches = 0
      events = []
      service = makeService({
        executions: { a: running(), b: running() },
        functions: {
          test_hangs: { args: ['event'], func: () => hung.promise },
          test_fine: { args: ['event'], func: async () => { otherFetches++; return { data: { n: otherFetches } } } }
        }
      })
      service.eventModel = { create: async doc => { events.push(doc) } }
      service.refreshTimeoutSeconds = 0.02 // 20ms, set directly for the test
      service.subscribe({ feed: 'slow', fetchFunction: 'test_hangs', executionName: 'a' })
      service.subscribe({ feed: 'quick', fetchFunction: 'test_fine', executionName: 'b' })
    })

    it('gives up on a feed that takes too long, and still refreshes the others', async () => {
      await service.runCycle()

      expect(otherFetches).to.eql(1)
      expect(service.statebox.heartbeats.map(h => h.name)).to.eql(['b'])

      const slow = service.status().feeds.find(f => f.feed === 'slow')
      expect(slow.lastError).to.eql('Timed out after 0.02s')
      expect(slow.waitingOnPreviousRefresh).to.eql(true)
      expect(events.find(e => e.event === 'FAILED')).to.include({ feed: 'slow', detail: 'Timed out after 0.02s' })
    })

    it('does not start another refresh of that feed while the first is still waiting', async () => {
      let calls = 0
      service.functions.functions.test_hangs.func = () => { calls++; return hung.promise }

      await service.runCycle()
      await service.runCycle()

      expect(calls).to.eql(1)
      expect(otherFetches).to.eql(2)
    })

    it('tries the feed again once a timed-out refresh has waited six times the limit', async () => {
      let now = T0
      service.now = () => now
      service.viewerTimeoutSeconds = 100000 // still being polled
      let calls = 0
      service.functions.functions.test_hangs.func = () => { calls++; return hung.promise }

      await service.runCycle()
      now += 0.02 * 1000 * 5 // waited 5x the limit: still skipped
      await service.runCycle()
      expect(calls).to.eql(1)

      now += 0.02 * 1000 * 2 // waited 7x the limit: tried again
      await service.runCycle()
      expect(calls).to.eql(2)
    })

    it('writes nothing when a timed-out refresh finishes late, then refreshes normally', async () => {
      await service.runCycle()

      hung.resolve({ data: { late: true } })
      await new Promise(resolve => setImmediate(resolve))
      expect(service.statebox.heartbeats.map(h => h.name)).to.eql(['b'])
      expect(service.status().feeds.find(f => f.feed === 'slow').waitingOnPreviousRefresh).to.eql(false)

      await service.runCycle()
      expect(service.statebox.heartbeats.filter(h => h.name === 'a').length).to.eql(1)
    })
  })

  describe('calling blueprint functions', () => {
    it('passes arguments by name, and supports callbacks', async () => {
      const service = makeService({
        functions: {
          test_named: { args: ['jobId', 'env'], func: (jobId, env) => ({ jobId, env }) },
          test_callback: { args: ['event', 'callback'], func: (event, callback) => callback(null, { got: event.x }) }
        }
      })

      const named = await service.callFunction('test_named', { event: { jobId: 7 } })
      expect(named.jobId).to.eql(7)
      expect(named.env).to.equal(service.env)

      const cb = await service.callFunction('test_callback', { event: { x: 1 } })
      expect(cb).to.eql({ got: 1 })
    })

    it('fails clearly for an unknown function', async () => {
      const service = makeService({ functions: {} })
      let error
      try { await service.callFunction('nope', { event: {} }) } catch (err) { error = err }
      expect(error.message).to.contain("'nope' not found")
    })
  })

  describe('change detection', () => {
    it('ignores timestamps and query times', () => {
      expect(hashOf({ a: 1, timestamp: 1, lastQueried: 1, generated: 1 }))
        .to.eql(hashOf({ a: 1, timestamp: 2, lastQueried: 2, generated: 2 }))
      expect(hashOf({ a: 1 })).to.not.eql(hashOf({ a: 2 }))
    })
  })

  describe('after a restart', () => {
    it('re-subscribes cards that were still open', async () => {
      const service = makeService({ functions: {} })
      const queries = []

      service.services = {
        statebox: {
          findStates: ({ resourceToFind }) => resourceToFind === 'module:subscribeToLiveFeed'
            ? [{ stateMachine: { name: 'test_getJobSummary_1_0' } }, { stateMachine: { name: 'test_getOpenJobs_1_0' } }]
            : []
        },
        storage: {
          models: {
            tymly_execution: {
              find: async ({ where }) => {
                queries.push(where)
                if (where.stateMachineName.equals !== 'test_getJobSummary_1_0') return []
                return [
                  {
                    executionName: 'open-card',
                    executionOptions: { userId: 'alice' },
                    ctx: { liveFeedSubscription: { feed: 'jobSummary', input: { jobId: 1 }, fetchFunction: 'f', viewerTimeoutSeconds: 90 } }
                  },
                  { executionName: 'opened-before-this-release', executionOptions: {}, ctx: {} }
                ]
              }
            }
          }
        }
      }

      const count = await service.resubscribeRunningViewers()

      expect(count).to.eql(1)
      expect(queries.map(q => q.status.equals)).to.eql(['RUNNING', 'RUNNING'])
      expect(service.listFeeds()).to.eql([{ key: 'jobSummary:{"jobId":1}', feed: 'jobSummary', viewers: ['open-card'] }])
    })

    it('does nothing without storage', async () => {
      const service = makeService({ functions: {} })
      expect(await service.resubscribeRunningViewers()).to.eql(0)
    })
  })

  describe('debugging', () => {
    function withEvents (service) {
      const events = []
      service.eventModel = { create: async doc => { events.push(doc) } }
      return events
    }

    let service, events

    beforeEach(() => {
      service = makeService({
        executions: { a: running(), b: running() },
        functions: { test_fetch: { args: ['event'], func: async () => ({ data: { value: 1 } }) } }
      })
      events = withEvents(service)
      service.subscribe({ feed: 'live', input: { n: 1 }, fetchFunction: 'test_fetch', executionName: 'a', userId: 'alice' })
    })

    it('records subscriptions and changes, not unchanged refreshes', async () => {
      await service.runCycle()
      await service.runCycle()

      expect(events.map(e => e.event)).to.eql(['SUBSCRIBED', 'CHANGED'])
      expect(events[1]).to.include({ feed: 'live', feedKey: 'live:{"n":1}', executionName: 'a', userId: 'alice' })
    })

    it('records viewers dropped and stopped', async () => {
      service.subscribe({ feed: 'live', input: { n: 1 }, fetchFunction: 'test_fetch', executionName: 'b', userId: 'bob' })
      service.subscribe({ feed: 'live', input: { n: 1 }, fetchFunction: 'test_fetch', executionName: 'c', userId: 'carol' })
      service.statebox = fakeStatebox({ a: running(), b: { status: 'STOPPED' }, c: running(T0 - 240000) })

      await service.runCycle()

      const byEvent = name => events.filter(e => e.event === name)
      expect(byEvent('DROPPED')[0]).to.include({ executionName: 'b', detail: 'execution STOPPED' })
      expect(byEvent('STOPPED')[0].executionName).to.eql('c')
      expect(byEvent('STOPPED')[0].detail).to.contain('not polled since')
    })

    it('records a failed fetch, and keeps it in the status', async () => {
      service.functions.functions.test_fetch.func = async () => { throw new Error('database down') }

      await service.runCycle()

      expect(events.at(-1)).to.include({ event: 'FAILED', detail: 'database down' })
      const feed = service.status().feeds[0]
      expect(feed.lastError).to.eql('database down')
      expect(feed.runs).to.eql(1)
    })
    it('records a feed that keeps failing the same way once, and again after it recovers', async () => {
      let fail = true
      service.functions.functions.test_fetch.func = async () => {
        if (fail) throw new Error('database down')
        return { data: { n: 1 } }
      }

      await service.runCycle()
      await service.runCycle()
      await service.runCycle()
      expect(events.filter(e => e.event === 'FAILED').length).to.eql(1)

      fail = false
      await service.runCycle()
      fail = true
      await service.runCycle()
      expect(events.filter(e => e.event === 'FAILED').length).to.eql(2)
    })

    it('records a finished feed', async () => {
      service.functions.functions.test_fetch.func = async () => ({ data: {}, finished: true })
      await service.runCycle()
      expect(events.at(-1).event).to.eql('FINISHED')
    })

    it('carries on if an event cannot be written', async () => {
      service.eventModel = { create: async () => { throw new Error('table missing') } }
      await service.runCycle()
      expect(service.statebox.heartbeats.length).to.eql(1)
    })

    it('reports feeds and viewers in the status', async () => {
      await service.runCycle()
      const status = service.status()

      expect(status.intervalSeconds).to.eql(30)
      expect(status.nextCycleInSeconds).to.eql(5)
      expect(status.feeds.length).to.eql(1)
      expect(status.feeds[0]).to.include({ key: 'live:{"n":1}', feed: 'live', fetchFunction: 'test_fetch', runs: 1, lastError: null })
      expect(status.feeds[0].lastRunAt).to.eql(new Date(T0).toISOString())
      expect(status.feeds[0].viewers[0]).to.include({ executionName: 'a', userId: 'alice' })
      expect(status.feeds[0].viewers[0].lastChangedAt).to.eql(new Date(T0).toISOString())
    })

    it('looks up each user\'s email once, and shows it in the status and the events', async () => {
      const lookups = []
      service.services = {
        userInfo: {
          emailFromUserId: async userId => {
            lookups.push(userId)
            return `${userId}@example.com`
          }
        }
      }
      service.subscribe({ feed: 'live', input: { n: 1 }, fetchFunction: 'test_fetch', executionName: 'b', userId: 'bob' })
      service.subscribe({ feed: 'live', input: { n: 1 }, fetchFunction: 'test_fetch', executionName: 'c', userId: 'bob' })
      await service.runCycle()

      expect(lookups).to.eql(['bob'])
      const viewers = service.status().feeds[0].viewers
      expect(viewers.find(v => v.executionName === 'b').userEmail).to.eql('bob@example.com')
      expect(events.filter(e => e.event === 'SUBSCRIBED' && e.userId === 'bob').map(e => e.userEmail)).to.eql(['bob@example.com', 'bob@example.com'])
      expect(events.filter(e => e.event === 'CHANGED' && e.userId === 'bob')[0].userEmail).to.eql('bob@example.com')
    })

    it('carries on without an email when it cannot be looked up', async () => {
      service.services = { userInfo: { emailFromUserId: async () => { throw new Error('Auth0 down') } } }
      service.subscribe({ feed: 'live', input: { n: 1 }, fetchFunction: 'test_fetch', executionName: 'b', userId: 'bob' })
      await service.runCycle()

      expect(service.status().feeds[0].viewers.find(v => v.executionName === 'b').userEmail).to.eql(null)
      expect(events.find(e => e.event === 'SUBSCRIBED' && e.userId === 'bob').userEmail).to.eql(null)
    })

    it('only lets users with the status role see the status', async () => {
      service.services = { rbac: { listUserRoles: async userId => userId === 'admin' ? ['realtime_admin'] : ['tymly_rbacAdmin'] } }

      expect(await service.canSeeStatus('admin')).to.eql(true)
      expect(await service.canSeeStatus('user')).to.eql(false)
      expect(await service.canSeeStatus(undefined)).to.eql(false)
    })

    it('serves the status at /live-feed/status, refusing other users', async () => {
      let prefix, handler
      const app = {
        jwtCheck: () => {},
        register: (router, options) => {
          prefix = options.prefix
          router({ jwtCheck: app.jwtCheck, get: (path, opts, fn) => { handler = { path, opts, fn } } })
        }
      }
      service.services = {
        server: { app },
        jwtAuth: { extractUserIdFromRequest: request => request.user.sub },
        rbac: { listUserRoles: async userId => userId === 'admin' ? ['realtime_admin'] : [] }
      }
      service.addStatusRoute()

      const reply = () => {
        const r = { statusCode: 200, body: null }
        r.code = code => { r.statusCode = code; return r }
        r.send = body => { r.body = body; return r }
        return r
      }

      expect(`${prefix}${handler.path}`).to.eql('/live-feed/status')
      expect(handler.opts.preValidation).to.eql([app.jwtCheck])

      const refused = await handler.fn({ user: { sub: 'user' } }, reply())
      expect(refused.statusCode).to.eql(403)

      const allowed = await handler.fn({ user: { sub: 'admin' } }, reply())
      expect(allowed.statusCode).to.eql(200)
      expect(allowed.body.feeds.length).to.eql(1)
    })

    it('deletes events older than the retention period', async () => {
      const queries = []
      service.services = { storage: { client: { query: async (sql, params) => queries.push({ sql, params }) } } }
      service.eventRetentionDays = 7

      await service.deleteOldEvents()

      expect(queries[0].sql).to.contain('DELETE FROM realtime.live_feed_event')
      expect(queries[0].params).to.eql([7])
    })
  })

  describe('settings', () => {
    function registry (values) {
      const reads = []
      return {
        reads,
        findById: async key => {
          reads.push(key)
          return key in values ? { key, value: { value: values[key] } } : undefined
        }
      }
    }

    it('has a registry key file in this plugin\'s blueprint for every setting, with the same environment variable and default', () => {
      const path = require('path')
      const fs = require('fs')
      const dir = path.join(__dirname, '..', 'lib', 'blueprints', 'realtime-blueprint')
      const namespace = JSON.parse(fs.readFileSync(path.join(dir, 'blueprint.json'), 'utf8')).namespace
      const camel = s => s.replace(/-([a-z])/g, (m, c) => c.toUpperCase())

      const files = {}
      for (const file of fs.readdirSync(path.join(dir, 'registry-keys'))) {
        files[`${namespace}_${camel(file.replace('.json', ''))}`] = JSON.parse(fs.readFileSync(path.join(dir, 'registry-keys', file), 'utf8'))
      }

      for (const setting of Object.values(SETTINGS)) {
        const file = files[setting.key]
        expect(file, setting.key).to.not.eql(undefined)
        expect(file.schema.properties.environmentVariableName).to.eql(setting.env)
        expect(file.schema.properties.value.default).to.eql(setting.default)
      }
      expect(Object.keys(files).length).to.eql(Object.keys(SETTINGS).length)
    })

    it('has the default status role as a template role in this plugin', () => {
      const fs = require('fs')
      const path = require('path')
      expect(SETTINGS.statusRole.default).to.eql('realtime_admin')
      expect(fs.existsSync(path.join(__dirname, '..', 'lib', 'blueprints', 'realtime-blueprint', 'template-roles', 'admin.json'))).to.eql(true)
    })

    it('has a registry key, environment variable and default for every setting', () => {
      const service = new LiveFeedService()
      for (const [name, setting] of Object.entries(SETTINGS)) {
        expect(setting.key).to.match(/^realtime_liveFeed/)
        expect(setting.env).to.match(/^LIVE_FEED_/)
        expect(service[name]).to.eql(setting.default)
      }
    })

    it('reads the settings from the registry', async () => {
      const service = makeService({ functions: {} })
      service.registryKeyModel = registry({
        realtime_liveFeedIntervalSeconds: 30,
        realtime_liveFeedOffsetSeconds: '5',
        realtime_liveFeedViewerTimeoutSeconds: 120,
        realtime_liveFeedEventRetentionDays: 14,
        realtime_liveFeedStatusRole: 'test_liveFeedAdmin',
        realtime_liveFeedDebug: 'true'
      })

      await service.loadSettings()

      expect(service.intervalSeconds).to.eql(30)
      expect(service.offsetSeconds).to.eql(5)
      expect(service.viewerTimeoutSeconds).to.eql(120)
      expect(service.eventRetentionDays).to.eql(14)
      expect(service.statusRole).to.eql('test_liveFeedAdmin')
      expect(service.debug).to.eql(true)
      expect(service.status().settings.intervalSeconds).to.eql(30)
    })

    it('keeps the current value when the registry value is missing or invalid', async () => {
      const service = makeService({ functions: {} })
      service.registryKeyModel = registry({
        realtime_liveFeedIntervalSeconds: 'soon',
        realtime_liveFeedOffsetSeconds: -1,
        realtime_liveFeedDebug: null
      })

      await service.loadSettings()

      expect(service.intervalSeconds).to.eql(30)
      expect(service.offsetSeconds).to.eql(10)
      expect(service.debug).to.eql(false)
    })

    it('uses the environment variables before the registry has been read', () => {
      process.env.LIVE_FEED_INTERVAL_SECONDS = '20'
      process.env.LIVE_FEED_DEBUG = 'true'
      try {
        const service = new LiveFeedService()
        expect(service.intervalSeconds).to.eql(20)
        expect(service.debug).to.eql(true)
      } finally {
        delete process.env.LIVE_FEED_INTERVAL_SECONDS
        delete process.env.LIVE_FEED_DEBUG
      }
    })

    it('does nothing while paused, and carries on once unpaused, without a restart', async () => {
      const values = { realtime_liveFeedPaused: true }
      let fetches = 0
      const service = makeService({
        executions: { a: running() },
        functions: { test_fetch: { args: ['event'], func: async () => { fetches++; return { data: { n: fetches } } } } }
      })
      service.registryKeyModel = registry(values)
      service.subscribe({ feed: 'live', fetchFunction: 'test_fetch', executionName: 'a' })

      await service.runCycle()
      await service.runCycle()
      expect(fetches).to.eql(0)
      expect(service.statebox.heartbeats).to.eql([])
      expect(service.status().paused).to.eql(true)
      expect(service.listFeeds()[0].viewers).to.eql(['a'])

      values.realtime_liveFeedPaused = false
      await service.runCycle()
      expect(fetches).to.eql(1)
      expect(service.statebox.heartbeats.length).to.eql(1)
      expect(service.status().paused).to.eql(false)
    })

    it('does not stop cards nobody is polling while paused', async () => {
      const service = makeService({
        executions: { a: running(T0 - 24 * 60 * 60 * 1000) },
        functions: { test_fetch: { args: ['event'], func: async () => ({ data: {} }) } }
      })
      service.registryKeyModel = registry({ realtime_liveFeedPaused: 'true' })
      service.subscribe({ feed: 'live', fetchFunction: 'test_fetch', executionName: 'a' })

      await service.runCycle()
      expect(service.statebox.stopped).to.eql([])
    })

    it('picks up a change at the start of the next cycle, without a restart', async () => {
      const values = { realtime_liveFeedIntervalSeconds: 60 }
      const service = makeService({ functions: {} })
      service.registryKeyModel = registry(values)

      await service.runCycle()
      expect(service.msUntilNextCycle(Date.parse('2026-11-10T12:00:05Z'))).to.eql(5000)

      values.realtime_liveFeedIntervalSeconds = 30
      values.realtime_liveFeedOffsetSeconds = 20
      await service.runCycle()

      expect(service.intervalSeconds).to.eql(30)
      expect(service.msUntilNextCycle(Date.parse('2026-11-10T12:00:05Z'))).to.eql(15000)
    })

    it('uses the viewer timeout setting unless the card overrides it', async () => {
      const service = makeService({
        executions: { a: running(T0 - 60000), b: running(T0 - 60000) },
        functions: { test_fetch: { args: ['event'], func: async () => ({ data: {} }) } }
      })
      service.registryKeyModel = registry({ realtime_liveFeedViewerTimeoutSeconds: 45 })

      service.subscribe({ feed: 'live', fetchFunction: 'test_fetch', executionName: 'a' })
      service.subscribe({ feed: 'live', fetchFunction: 'test_fetch', executionName: 'b', viewerTimeoutSeconds: 300 })

      await service.runCycle()

      expect(service.statebox.stopped).to.eql(['a'])
      expect(service.listFeeds()[0].viewers).to.eql(['b'])
    })

    it('cleans up old events on boot and then once a day', () => {
      const service = makeService({ functions: {} })
      let cleanups = 0
      service.deleteOldEvents = async () => { cleanups++ }

      let now = T0
      service.now = () => now

      service.cleanUpEventsIfDue()
      now += 60 * 60 * 1000
      service.cleanUpEventsIfDue()
      now += 24 * 60 * 60 * 1000
      service.cleanUpEventsIfDue()

      expect(cleanups).to.eql(2)
    })
  })

  describe('shutting down', () => {
    it('cancels the next cycle and clears the feeds', () => {
      const service = makeService({ functions: {} })
      service.scheduleNextCycle()
      service.subscribe({ feed: 'x', fetchFunction: 'f', executionName: 'a' })
      service.shutdown()

      expect(service.timer).to.eql(null)
      expect(service.listFeeds()).to.eql([])
    })
  })
})
