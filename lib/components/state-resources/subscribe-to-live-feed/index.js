// Subscribes the current execution (a card waiting for the user) to a shared live feed,
// instead of launching its own refresh loop. See the liveFeed service.
//
// ResourceConfig:
//   feed                  name of the feed, e.g. "jobSummary"
//   fetchFunction         blueprint function that gets the shared data, e.g. "myapp_liveJobSummary".
//                         Called with the state's input as event, returns { data, finished }
//   presentFunction       optional blueprint function run for each viewer, e.g. to hide sensitive details.
//                         Called with the fetched data as event and { userId } as context, returns the data
//   intervalSeconds       optional, how often this feed refreshes. Otherwise the
//                         realtime_liveFeedIntervalSeconds registry key applies. Refreshes are aligned
//                         to the clock, offset by realtime_liveFeedOffsetSeconds
//   viewerTimeoutSeconds  optional, stop the execution when nobody has polled it for this long.
//                         Normally left out, so the realtime_liveFeedViewerTimeoutSeconds registry key applies
//
// The state's input (e.g. { jobId }) identifies the feed: viewers with the same feed name and
// input share one feed.
//
// Returns the subscription, which the state should keep in the context with
// "ResultPath": "$.liveFeedSubscription", so the liveFeed service can re-subscribe the card if
// Tymly restarts while it's open.

module.exports = class SubscribeToLiveFeed {
  init (resourceConfig, env) {
    // looked up when it runs: state resources are set up while statebox boots, before the
    // liveFeed service (which boots after statebox) exists
    this.services = env.bootedServices
    this.feed = resourceConfig.feed
    this.fetchFunction = resourceConfig.fetchFunction
    this.presentFunction = resourceConfig.presentFunction
    this.intervalSeconds = resourceConfig.intervalSeconds
    this.viewerTimeoutSeconds = resourceConfig.viewerTimeoutSeconds
  }

  run (event, context) {
    try {
      const { liveFeed } = this.services

      if (!liveFeed) {
        throw new Error('The liveFeed service is not running')
      }

      const subscription = {
        feed: this.feed,
        input: event || {},
        fetchFunction: this.fetchFunction,
        presentFunction: this.presentFunction,
        intervalSeconds: this.intervalSeconds,
        viewerTimeoutSeconds: this.viewerTimeoutSeconds
      }

      liveFeed.subscribe({
        ...subscription,
        executionName: context.executionName,
        userId: context.userId
      })

      return context.sendTaskSuccess(subscription)
    } catch (err) {
      return context.sendTaskFailure({ error: 'subscribeToLiveFeed', cause: err.message })
    }
  }
}
