package wafna.kadabra

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.Timer
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.CancellationException
import java.io.Closeable
import java.sql.Connection
import java.sql.SQLTransientConnectionException
import java.util.*
import kotlin.time.Duration

/**
 * For visibility into connection management and usage.
 */
interface DBListener : RetryListener {
    /**
     * Connection requested.
     */
    suspend fun request(id: UUID)

    /**
     * Connection granted.
     */
    suspend fun connect(id: UUID)

    /**
     * Transaction committed.
     */
    suspend fun commit(id: UUID)

    /**
     * Transaction rolled back.
     */
    suspend fun rollback(id: UUID)
}

/**
 * Does nothing.
 */
val DBListenerNOOP = object : DBListener {
    override suspend fun request(id: UUID) = Unit
    override suspend fun connect(id: UUID) = Unit
    override suspend fun commit(id: UUID) = Unit
    override suspend fun rollback(id: UUID) = Unit
    override suspend fun cancelled(e: CancellationException) = Unit
    override suspend fun retrying(nthTry: Int, maxAttempts: Int, e: Throwable, delay: Duration) = Unit
    override suspend fun retriesExceeded(maxAttempts: Int, e: Throwable) = Unit
    override suspend fun notRetryable(e: Throwable) = Unit
}

/**
 * All the settings.
 */
data class DBConfig(
    /**
     * Maximum number of times to attempt an operation on a connection.
     */
    val maxAttempts: Int,
    /**
     * Strategy for delaying between attempts.
     */
    val delayStrategy: DelayStrategy,
    /**
     * Minimum elapsed time between transactions.
     */
    val throttle: Duration,
    val hikariConfig: HikariConfig
)

/**
 * Borrow a DB.
 */
suspend fun runDB(config: DBConfig, listener: DBListener = DBListenerNOOP, borrow: suspend (DB) -> Unit) {
    DB(config, listener).use { db ->
        borrow(db)
    }
}

/**
 * Provides a wrapper around a connection pool along with transaction management, rate limiting, and retries.
 */
class DB private constructor(
    private val dataSource: HikariDataSource,
    private val maxAttempts: Int,
    private val delayStrategy: DelayStrategy,
    throttleDT: Duration,
    private val listener: DBListener = DBListenerNOOP
) : Closeable {
    constructor(config: DBConfig, listener: DBListener = DBListenerNOOP) : this(
        HikariDataSource(config.hikariConfig),
        config.maxAttempts,
        config.delayStrategy,
        config.throttle,
        listener
    )

    private val throttle = Throttle(throttleDT)

    private val metricsRegistry = MetricRegistry()
    private val connectionWait: Timer = metricsRegistry.timer("connection-wait")
    private val connectionUse: Timer = metricsRegistry.timer("connection-use")

    /**
     * Get a connection for use in a single transaction.
     * If the borrowing function returns normally the transaction is committed, otherwise, it is rolled back.
     */
    suspend fun <T> connect(borrow: suspend (connection: Connection) -> T): T = throttle.flow {
        retry(delayStrategy, maxAttempts, listOf(SQLTransientConnectionException::class), listener) {
            val id = UUID.randomUUID()
            listener.request(id)
            val beginWait = System.currentTimeMillis()
            dataSource.connection.use { cx ->
                listener.connect(id)
                connectionWait.updateInterval(beginWait)
                val beginUse = System.currentTimeMillis()
                cx.autoCommit = false
                cx.beginRequest()
                try {
                    borrow(cx).also {
                        listener.commit(id)
                        cx.commit()
                    }
                } catch (e: Throwable) {
                    listener.rollback(id)
                    cx.rollback()
                    throw e
                } finally {
                    cx.endRequest()
                    connectionUse.updateInterval(beginUse)
                }
            }
        }
    }

    /**
     * Closes the connection pool.
     */
    override fun close() {
        dataSource.close()
    }
}