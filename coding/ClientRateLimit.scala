/*

Problem: Design a rate limiter that limits the number of requests a client can make per minute to 100. If the number of requests a client makes within a minute is greater than 100, all following requests within that timeframe are dropped. All rate limits should vary separately by client.

Questions/Clarifications/Assumptions:
1. If a request is dropped, just print something to standard output.
2. Single threaded service that handles one request at a time.
3. Should we create a whitelist of accepted clients? If so, how should we handle clients that we don't recognize?
*/

trait RateLimiter {
    def acceptsRequest(): Boolean
    def copy(): RateLimiter
}

/**
 * One solution is to use the Token Bucket algorithm. This algorithm allocates a set
 * number of "tokens" that are "spent" on receipt of a new request. The tokens are replenished
 * at a set rate defined in the implementation.
 */

/**
 * maxTokens: maximum number of tokens that can be allocated 
 * timeframe: the timeframe in milliseconds over which maxTokens can be spent before being 
 * depleted. i.e.: maxTokens=100, timeframe=60_000, means that that 100 tokens can be spent 
 * every minute.
 */
class TokenBucket(val maxTokens: Long, val timeframe: Long) extends RateLimiter {
    // Assumption here is that this class is only used in a single-threaded context
    var availableTokens: Long = maxTokens
    var lastRefresh: Long = System.currentTimeMillis()
    val refreshRate: Double = maxTokens.toDouble / timeframe.toDouble

    override def acceptsRequest(): Boolean = spendToken()
    
    override def copy() = {
        TokenBucket(maxTokens, timeframe)
    }

    /**
     * Spends token if one is available. Returns true if token has been spent, else false and
     * token can't be spent.
     */
    def spendToken(): Boolean = {
        refillBucket()
        if (availableTokens > 0) {
            availableTokens -= 1
            true
        } else {
            false
        }
    }
    private[this] def refillBucket(): Unit = {
        val now = System.currentTimeMillis()
        val replenishedTokens = (refreshRate * (now - lastRefresh)).toLong
        availableTokens = math.min(maxTokens, availableTokens + replenishedTokens)
        lastRefresh = now
    }
}

val whitelistedClientIds = Set("client1", "client2", "client3")

class Server(rateLimiter: RateLimiter) {
    val clientMap = whitelistedClientIds.map(id => id -> rateLimiter.copy()).toMap
    def handleRequest(clientId: String): Boolean = {
        clientMap.get(clientId) match {
            case Some(clientRateLimiter: RateLimiter) => clientRateLimiter.acceptsRequest()
            case None => false
        }
    }
}

val server = Server(
    rateLimiter=TokenBucket(maxTokens=100, timeframe=60_000)
)

val assertRequestsDropWhenExpected = () => {
    (1 to 100).foreach(_ => {
            assert(server.handleRequest(whitelistedClientIds.head) == true)
        }
    )
    assert(server.handleRequest(whitelistedClientIds.head) == false)
    Thread.sleep(600) // This is the number of milliseconds it takes before a single token is regenerated (i.e. 1/refreshRate(token/ms) = 600ms/token)
    assert(server.handleRequest(whitelistedClientIds.head) == true)
}

val assertUnexpectedClientDoesnHandleRequest = () => {
    assert(server.handleRequest("unexpected-client-id") == false)
}

assertRequestsDropWhenExpected()
assertUnexpectedClientDoesnHandleRequest()