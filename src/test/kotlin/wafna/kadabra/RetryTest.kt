package wafna.kadabra

import kotlin.math.pow
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds
import kotlin.time.times

class RetryTest {
    @Test
    fun testDelayConst() {
        val ds = DelayConst(1.seconds)
        repeat(3) {
            assertEquals(1.seconds, ds.wait(1 + it))
        }
    }

    @Test
    fun testDelayArithmetic() {
        val ds = DelayArithmetic(1.seconds)
        repeat(3) {
            assertEquals((1 + it) * 1.seconds, ds.wait(1 + it))
        }
    }

    @Test
    fun testDelayGeometric() {
        val ds = DelayGeometric(1.seconds, 2.0)
        repeat(3) {
            assertEquals(1.seconds * 2.0.pow(it.toDouble()), ds.wait(1 + it))
        }
    }
}