package wafna.kadabra

import com.codahale.metrics.Snapshot
import com.codahale.metrics.Timer
import java.util.concurrent.TimeUnit

fun Timer.updateInterval(start: Long, end: Long = System.currentTimeMillis()) {
    require(start <= end)
    update(end - start, TimeUnit.MILLISECONDS)
}

suspend fun <T> Timer.scope(block: suspend () -> T): T {
    val start = System.currentTimeMillis()
    return block().also {
        updateInterval(start)
    }
}

data class StatsSnapshot(
    val size: Int,
    val median: Double,
    val p75: Double,
    val p95: Double,
    val p98: Double,
    val p99: Double,
    val p999: Double,
    val max: Long,
    val min: Long,
    val mean: Double,
    val stdDev: Double
) {
    companion object {
        fun fromSnapshot(s: Snapshot): StatsSnapshot =
            StatsSnapshot(
                s.size(),
                s.median,
                s.get75thPercentile(),
                s.get95thPercentile(),
                s.get98thPercentile(),
                s.get99thPercentile(),
                s.get999thPercentile(),
                s.max,
                s.min,
                s.mean,
                s.stdDev
            )
    }
}

data class RateSnapshot(
    val count: Long,
    val meanRate: Double,
    val oneMinuteRate: Double,
    val fiveMinuteRate: Double,
    val fifteenMinuteRate: Double
)

data class ConnectionMetrics(
    val wait: StatsSnapshot,
    val use: StatsSnapshot,
    val rate: RateSnapshot,
) {
    constructor(wait: Timer, use: Timer) : this(
        wait = StatsSnapshot.fromSnapshot(wait.snapshot),
        use = StatsSnapshot.fromSnapshot(use.snapshot),
        rate = RateSnapshot(
            wait.count,
            wait.meanRate,
            wait.oneMinuteRate,
            wait.fiveMinuteRate,
            wait.fifteenMinuteRate
        )
    )
}