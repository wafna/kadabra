package wafna.kadabra

import com.zaxxer.hikari.HikariConfig
import kotlinx.coroutines.runBlocking
import java.sql.Timestamp
import java.time.Instant
import java.util.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds

data class Thingy(val id: UUID, val name: String, val integer: Int, val double: Double, val tstamp: Timestamp)

object Entities {
    val thingy = Entity(
        tableName = "thingy",
        columnNames = listOf("id", "name", "integer", "double", "tstamp"),
        fieldNames = listOf("id", "name", "integer", "double", "tstamp")
    )
}

// Create the schema for our testing database.
suspend fun DB.initThingies() {
    connect { cx ->
        cx.update("DROP TABLE IF EXISTS ${Entities.thingy.tableName}")
        cx.update(
            """CREATE TABLE ${Entities.thingy.tableName} (
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
            jdbcUrl = "jdbc:h2:mem:test-1;DB_CLOSE_DELAY=-1" // "jdbc:h2:./etc/test-1"
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
                    cx.insert(Entities.thingy, inT)
                    cx.unique<Thingy>("""SELECT ${Entities.thingy.projection()} FROM ${Entities.thingy.tableName}""")!!
                        .also { outT ->
                            assertEquals(inT.normalize(), outT.normalize())
                        }
                    cx.unique<Thingy>(
                        """SELECT ${Entities.thingy.projection()} 
                          |  FROM ${Entities.thingy.tableName}
                          | WHERE integer = ?
                          |   AND double = ?
                          |   AND name = ?
                          |   AND id = ?""".trimMargin()
                    ) {
                        addInt(inT.integer)
                        addDouble(inT.double)
                        addStrings(inT.name)
                        addObject(inT.id)
                    }!!.also { outT ->
                        assertEquals(inT.normalize(), outT.normalize())
                    }
                    Timestamp.from(Instant.now().minusMillis(1.hours.inWholeMilliseconds)).also { ts ->
                        cx.update("""UPDATE ${Entities.thingy.tableName} SET tstamp = ? WHERE id = ?""") {
                            addTimestamp(ts)
                            addObject(inT.id)
                        }
                        cx.unique<Thingy>("""SELECT ${Entities.thingy.projection()} FROM ${Entities.thingy.tableName}""")!!
                            .also { outT ->
                                assertEquals(ts.normalize(), outT.tstamp.normalize())
                            }
                    }
                }
            }
        }
    }
}