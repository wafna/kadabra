package wafna.kadabra

import com.zaxxer.hikari.HikariConfig
import kotlinx.coroutines.runBlocking
import java.math.BigDecimal
import java.sql.Timestamp
import java.time.Instant
import java.util.*
import kotlin.reflect.KClass
import kotlin.reflect.KParameter
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.test.*
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds

data class Thingy1(
    val id: UUID,
    val name: String,
    val integer: Int,
    val double: Double,
    val tstamp: Timestamp,
    val guid: UUID
)

data class Thingy2(
    val id: UUID,
    val name: String?,
    val integer: Int?,
    val double: Double?,
    val tstamp: Timestamp?,
    val guid: UUID?
)

object Entities {
    val thingy1 = Entity(
        tableName = "thingy",
        columnNames = listOf("id", "name", "integer", "double", "tstamp", "guid"),
        fieldNames = listOf("id", "name", "integer", "double", "tstamp", "guid")
    )
    val thingy2 = Entity(
        tableName = "thingy",
        columnNames = listOf("id", "name", "integer", "double", "tstamp", "guid"),
        fieldNames = listOf("id", "name", "integer", "double", "tstamp", "guid")
    )
}

// Create the schema for our testing database.
suspend fun DB.initThingies() {
    connect { cx ->
        cx.update("DROP TABLE IF EXISTS ${Entities.thingy1.tableName}")
        cx.update(
            """CREATE TABLE ${Entities.thingy1.tableName} (
                            id UUID PRIMARY KEY,
                            name VARCHAR(32),
                            integer INTEGER,
                            double DOUBLE,
                            tstamp TIMESTAMP,
                            guid UUID
                            )"""
        )
    }
}

// The nanos resolution of the timestamp gets lost in the round trip to the database.
fun Timestamp.normalize(): Timestamp = Timestamp.from(Instant.ofEpochMilli(time))

fun Thingy1.normalize(): Thingy1 = copy(tstamp = tstamp.normalize())

class DBTest {
    /**
     * Check that matching on return types of properties works as expected.
     */
    @Test
    fun testWriteTypeMatching() {
        data class Datum(
            val propString: String,
            val propStringQ: String?,
            val propInt: Int,
            val propIntQ: Int?,
            val propLong: Long,
            val propLongQ: Long?,
            val propDouble: Double,
            val propDoubleQ: Double?,
            val propBigDecimal: BigDecimal,
            val propTimestamp: Timestamp
        )

        val props = Datum::class.declaredMemberProperties
        fun testProperty(propName: String, propClass: KClass<*>) {
            val prop = props.firstOrNull { it.name == propName } ?: fail("Property $propName not found.")
            assert(prop.returnType.classifier == propClass)
        }
        testProperty("propString", String::class)
        testProperty("propStringQ", String::class)
        testProperty("propInt", Int::class)
        testProperty("propIntQ", Int::class)
        testProperty("propLong", Long::class)
        testProperty("propLongQ", Long::class)
        testProperty("propDouble", Double::class)
        testProperty("propDoubleQ", Double::class)
        testProperty("propBigDecimal", BigDecimal::class)
        testProperty("propTimestamp", Timestamp::class)
    }

    @Test
    fun testReadTypeMatching() {
        data class Datum(
            val propString: String,
            val propStringQ: String?,
            val propInt: Int,
            val propIntQ: Int?,
            val propDouble: Double,
            val propDoubleQ: Double?,
            val propBigDecimal: BigDecimal,
            val propTimestamp: Timestamp
        )

        val parameters: List<KParameter> = Datum::class.primaryConstructor!!.parameters
        fun testProperty(propName: String, propClass: KClass<*>) {
            val parameter = parameters.firstOrNull { it.name == propName } ?: fail("Parameter $propName not found.")
            assert(parameter.type.classifier == propClass) { "${parameter.type.classifier} == $propClass" }
        }
        testProperty("propString", String::class)
        testProperty("propStringQ", String::class)
        testProperty("propInt", Int::class)
        testProperty("propIntQ", Int::class)
        testProperty("propDouble", Double::class)
        testProperty("propDoubleQ", Double::class)
        testProperty("propBigDecimal", BigDecimal::class)
        testProperty("propTimestamp", Timestamp::class)
    }

    @Test
    fun testNotNull() {
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
                    val inT = Thingy1(
                        UUID.randomUUID(),
                        "thing-1",
                        42,
                        6.023e23,
                        Timestamp.from(Instant.now()),
                        UUID.randomUUID()
                    )
                    cx.insert(Entities.thingy1, inT)
                    cx.unique<Thingy1>("""SELECT ${Entities.thingy1.projection()} FROM ${Entities.thingy1.tableName}""")!!
                        .also { outT ->
                            assertEquals(inT.normalize(), outT.normalize())
                        }
                    cx.unique<Thingy1>(
                        """SELECT ${Entities.thingy1.projection()} 
                          |  FROM ${Entities.thingy1.tableName}
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
                        cx.update("""UPDATE ${Entities.thingy1.tableName} SET tstamp = ? WHERE id = ?""") {
                            addTimestamp(ts)
                            addObject(inT.id)
                        }
                        cx.unique<Thingy1>("""SELECT ${Entities.thingy1.projection()} FROM ${Entities.thingy1.tableName}""")!!
                            .also { outT ->
                                assertEquals(ts.normalize(), outT.tstamp.normalize())
                            }
                    }
                    cx.list<Thingy1>("SELECT ${Entities.thingy1.projection()} FROM ${Entities.thingy1.tableName}")
                        .also {
                            assertEquals(1, it.size)
                        }
                }
            }
        }
    }

    @Test
    fun testNull() {
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
                    val id = UUID.randomUUID()
                    val inT = Thingy2(id, null, null, null, null, null)
                    cx.insert(Entities.thingy2, inT)
                    cx.unique<Thingy2>("SELECT ${Entities.thingy2.projection()} FROM ${Entities.thingy2.tableName}")!!
                        .also { outT ->
                            assertEquals(inT, outT)
                        }
                    cx.list<Thingy2>("SELECT ${Entities.thingy1.projection()} FROM ${Entities.thingy1.tableName}")
                        .also {
                            assertEquals(1, it.size)
                        }
                    cx.list<Thingy2>(
                        """SELECT ${Entities.thingy1.projection()}
                          |  FROM ${Entities.thingy1.tableName}
                          | WHERE id = ?""".trimMargin()
                    ) {
                        addObject(id)
                    }.also {
                        assertEquals(1, it.size)
                    }
                    cx.count("SELECT COUNT(*) FROM ${Entities.thingy1.tableName}")
                        .also {
                            assertEquals(1, it)
                        }
                    cx.count(
                        """SELECT COUNT(*)
                          |  FROM ${Entities.thingy1.tableName}
                          | WHERE id = ?""".trimMargin()
                    ) {
                        addObject(id)
                    }.also {
                        assertEquals(1, it)
                    }
                }
            }
        }
    }
}