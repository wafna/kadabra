package wafna.kadabra

import com.zaxxer.hikari.HikariConfig
import kotlinx.coroutines.runBlocking
import java.sql.Timestamp
import java.time.Instant
import java.util.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.milliseconds

data class Thingy(val id: UUID, val name: String, val integer: Int, val double: Double, val tstamp: Timestamp)

val thingy = Entity(
    tableName = "thingy",
    columnNames = listOf("id", "name", "integer", "double", "tstamp"),
    fieldNames = listOf("id", "name", "integer", "double", "tstamp")
)

// Create the schema for our
suspend fun DB.initThingies() {
    connect { cx ->
        cx.update("DROP TABLE IF EXISTS ${thingy.tableName}")
        cx.update(
            """CREATE TABLE ${thingy.tableName} (
                            id UUID PRIMARY KEY,
                            name VARCHAR(32) NOT NULL,
                            integer INTEGER,
                            double DOUBLE,
                            tstamp TIMESTAMP
                            )"""
        )
    }
}

// The nanos resolution of the timestamp gets lost in the round trip to the database.
fun Timestamp.normalize(): Timestamp = Timestamp.from(Instant.ofEpochMilli(time))

fun Thingy.normalize(): Thingy = copy(tstamp = tstamp.normalize())

class DBTest {
    @Test
    fun test1() {
        val hikariConfig = HikariConfig().apply {
            maximumPoolSize = 4
            jdbcUrl = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1" // "jdbc:h2:~/test-1"
            username = "sa"
            password = ""
        }
        val dbConfig = DBConfig(
            maxAttempts = 1,
            delayStrategy = DelayConst(0.milliseconds),
            throttle = 0.milliseconds,
            hikariConfig = hikariConfig
        )
        runBlocking {
            runDB(dbConfig) { db ->
                db.initThingies()
                db.connect { cx ->
                    val inT = Thingy(UUID.randomUUID(), "thing-1", 42, 6.023e23, Timestamp.from(Instant.now()))
                    cx.insert(thingy, inT)
                    val outT = cx.unique<Thingy>("""SELECT ${thingy.columnNames.project()} FROM ${thingy.tableName}""")!!
                    assertEquals(inT.normalize(), outT.normalize())
                }
            }
        }
    }
}