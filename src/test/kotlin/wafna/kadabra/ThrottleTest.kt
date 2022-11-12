package wafna.kadabra

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.milliseconds

class ThrottleTest {
    @Test
    fun testThrottle() {
        val dt = 100.milliseconds
        val throttle = Throttle(dt)
        val count = AtomicInteger(0)
        val start = System.currentTimeMillis()
        val reps = 8
        runBlocking(Executors.newFixedThreadPool(4).asCoroutineDispatcher()) {
            (1..reps).map {
                async { throttle.flow { count.incrementAndGet() } }
            }.awaitAll()
        }
        assertEquals(reps, count.get(), "Didn't do enough.")
        assert(System.currentTimeMillis() >= start + (reps * dt.inWholeMilliseconds)) { "Didn't take long enough." }
    }
}