package wafna.kadabra

import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.math.max
import kotlin.time.Duration

/**
 * Provides for limiting the rate of some activity, e.g. calling an API.
 * This is thread safe.
 */
class Throttle(minInterval: Duration) {

    private val intervalMS = minInterval.inWholeMilliseconds

    // The time at which the most recent call is scheduled to execute.
    // Note that this can be in the future.
    private var lastCall = 0L
    private val callMutex = Mutex()

    /**
     * Delays until the minimum interval between calls has elapsed and then calls the provided function.
     */
    suspend fun <T> throttle(fn: suspend () -> T): T {
        val now = System.currentTimeMillis()
        val dt = callMutex.withLock {
            val target = lastCall + intervalMS
            lastCall = max(target, now)
            target - now
        }
        if (0 < dt) {
            delay(dt)
        }
        return fn()
    }
}
