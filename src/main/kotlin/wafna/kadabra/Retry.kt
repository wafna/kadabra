package wafna.kadabra

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlin.math.pow
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf
import kotlin.time.Duration
import kotlin.time.times

/**
 * Visibility into retries.
 */
interface RetryListener {
    suspend fun cancelled(e: CancellationException)
    suspend fun retrying(nthTry: Int, maxAttempts: Int, e: Throwable, delay: Duration)
    suspend fun retriesExceeded(maxAttempts: Int, e: Throwable)
    suspend fun notRetryable(e: Throwable)
}

/**
 * Does nothing.
 */
object RetryListenerNOOP : RetryListener {
    override suspend fun cancelled(e: CancellationException) = Unit
    override suspend fun retrying(nthTry: Int, maxAttempts: Int, e: Throwable, delay: Duration) = Unit
    override suspend fun retriesExceeded(maxAttempts: Int, e: Throwable) = Unit
    override suspend fun notRetryable(e: Throwable) = Unit
}

/**
 * Allows for various strategies like exponential back off.
 * Implementations of this class should be stateless.
 */
abstract class DelayStrategy {
    /**
     * @param nthTry Will be one when this is called for the first time (for a given operation).
     */
    abstract fun wait(nthTry: Int): Duration
}

class DelayConst(private val duration: Duration) : DelayStrategy() {
    init {
        require(0 <= duration.inWholeMilliseconds && duration.isFinite()) { "Duration must by non-negative and finite." }
    }

    override fun wait(nthTry: Int): Duration = duration
}

class DelayArithmetic(private val duration: Duration) : DelayStrategy() {
    init {
        require(0 <= duration.inWholeMilliseconds && duration.isFinite()) { "Duration must by non-negative and finite." }
    }

    override fun wait(nthTry: Int): Duration = nthTry * duration
}

class DelayGeometric(val baseDuration: Duration, val factor: Double) : DelayStrategy() {
    init {
        require(0 <= baseDuration.inWholeMilliseconds && baseDuration.isFinite()) { "Duration must by non-negative and finite." }
        require(1.0 <= factor) { "Factor must not be less than one." }
    }

    override fun wait(nthTry: Int): Duration = baseDuration * factor.pow((nthTry - 1).toDouble())
}

/**
 * Retries the given operation some number of times if the operation throws a retryable exception, waiting in between tries.
 * A CancellationException will never trigger a retry, even if it's in the list of retryable exception types.
 * Retried exceptions will be instances or subclasses of the retryable exceptions.
 *
 * @maxAttempts The maximum number of times the operation will be attempted.
 * @wait The time to wait between tries.
 * @retryables The (super) classes of exceptions that will trigger a retry of the operation.
 */
suspend fun <T> retry(
    delayStrategy: DelayStrategy,
    maxAttempts: Int,
    retryables: List<KClass<out Throwable>>,
    listener: RetryListener = RetryListenerNOOP,
    op: suspend () -> T
): T {
    require(0 < maxAttempts)
    require(retryables.isNotEmpty())
    suspend fun retry(nthTry: Int): T = coroutineScope {
        try {
            op()
        } catch (e: CancellationException) {
            // CancellationException must always get through.
            listener.cancelled(e)
            throw e
        } catch (e: Throwable) {
            val ec = e::class
            if (retryables.any { ec.isSubclassOf(it) }) {
                if (nthTry < maxAttempts) {
                    val dt = delayStrategy.wait(nthTry)
                    listener.retrying(nthTry, maxAttempts, e, dt)
                    delay(dt)
                    retry(nthTry + 1)
                } else {
                    listener.retriesExceeded(maxAttempts, e)
                    throw e
                }
            } else {
                listener.notRetryable(e)
                throw e
            }
        }
    }
    return retry(1)
}