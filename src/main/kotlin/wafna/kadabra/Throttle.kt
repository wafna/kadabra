package wafna.kadabra

import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.math.max
import kotlin.time.Duration

/**
 * Provides for limiting the rate of some activity, e.g. calling an API,
 * by enforcing a minimum interval between actions.
 * @param minInterval Must be non-negative (can be zero).
 */
class Throttle(minInterval: Duration) {

    init {
        require(minInterval.isFinite() && ! minInterval.isNegative())
    }

    private val intervalMS = minInterval.inWholeMilliseconds

    // The time at which the most recent call is scheduled to execute.
    // Note that this can be in the future.
    private var lastCall = 0L
    private val mutex = Mutex()

    /**
     * Delays until the minimum interval between calls has elapsed and then calls the provided function.
     * This is thread safe.
     */
    suspend fun <T> flow(fn: suspend () -> T): T {
        val now = System.currentTimeMillis()
        mutex.withLock {
            val target = lastCall + intervalMS
            lastCall = max(target, now)
            val dt = target - now
            if (0 < dt) {
                delay(dt)
            }
        }
        return fn()
    }
}
