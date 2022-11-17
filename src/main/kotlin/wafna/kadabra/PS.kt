package wafna.kadabra

import java.lang.reflect.Constructor
import java.lang.reflect.Parameter
import java.math.BigDecimal
import java.sql.*
import java.time.Instant
import java.util.*
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.javaConstructor
import kotlin.reflect.jvm.jvmName

/**
 * Converts a collection to a map by applying a key generating function to each element.
 * Fails on key collisions.
 */
fun <T : Any, R : Any> Collection<T>.toMapStrict(key: (T) -> R): Map<R, T> = fold(TreeMap<R, T>()) { map, elem ->
    val k = key(elem)
    if (map.containsKey(k))
        throw RuntimeException("Duplicate key: $k")
    map[k] = elem
    map
}

class DBException(message: String, cause: Throwable? = null) : Exception(message, cause)

/**
 * Information needed to map on object into and out of a database.
 */
open class Entity(val tableName: String, val columnNames: List<String>, val fieldNames: List<String>) {
    /**
     * All the columns (in order), qualified with `prefix`, in a comma separated list.
     * Useful for SELECT and INSERT.
     */
    fun projection(prefix: String = tableName) = columnNames.qualify(prefix).project()
    val fieldMap = columnNames.zip(fieldNames)
}

/**
 * Sets a value, held in a closure, into the prepared statement at the indicated position.
 */
typealias SQLParam = (preparedStatement: PreparedStatement, position: Int) -> Unit

fun inList(size: Int): String = List(size) { "?" }.joinToString()
fun inList(items: Collection<Any?>): String = inList(items.size)

fun List<String>.qualify(tableName: String): List<String> =
    map { "$tableName.$it" }

fun List<String>.project(): String = joinToString(", ")

// Turn values into params for prepared statements.

val Int.sql: SQLParam
    get() = { ps, position -> ps.setInt(position, this) }
val Double.sql: SQLParam
    get() = { ps, position -> ps.setDouble(position, this) }
val String.sql: SQLParam
    get() = { ps, position -> ps.setString(position, this) }
val Instant.sql: SQLParam
    get() = { ps, position -> ps.setDate(position, java.sql.Date(this.toEpochMilli())) }
val Timestamp.sql: SQLParam
    get() = { ps, position -> ps.setTimestamp(position, this) }
val BigDecimal.sql: SQLParam
    get() = { ps, position -> ps.setBigDecimal(position, this) }
val Any.sql: SQLParam
    get() = { ps, position -> ps.setObject(position, this) }

/**
 * Collection of parameters to prepared statements.
 */
class Params {
    private val params = mutableListOf<SQLParam>()

    fun array(): Array<SQLParam> = params.toTypedArray()

    fun add(param: SQLParam) = params.add(param)
    fun add(params: Collection<SQLParam>) = params.forEach(::add)

    fun addInt(p: Int) = params.add(p.sql)
    fun addDouble(p: Double) = params.add(p.sql)
    fun addString(p: String) = params.add(p.sql)
    fun addStrings(ps: Collection<String>) = ps.forEach { params.add(it.sql) }
    fun addStrings(vararg ps: String) = ps.forEach { params.add(it.sql) }
    fun addBigDecimal(p: BigDecimal) = params.add(p.sql)

    // fun addBigDecimals(ps: Collection<BigDecimal>) = ps.forEach { params.add(it.sql) }
    fun addTimestamp(p: Timestamp) = params.add(p.sql)
    fun addTimestamp(p: Instant) = params.add(Timestamp(p.toEpochMilli()).sql)
    fun addObject(p: Any) = params.add(p.sql)
}

/**
 * Interpolates positional parameters into a prepared statement.
 */
fun PreparedStatement.setParams(vararg params: SQLParam): PreparedStatement = also {
    params.withIndex().forEach { it.value(this, 1 + it.index) }
}

/**
 * Interpolates positional parameters into a prepared statement.
 */
fun PreparedStatement.setParams(params: Params): PreparedStatement = also {
    params.array().withIndex().forEach { it.value(this, 1 + it.index) }
}

/**
 * Gets a value from a record set at a position, which position is held in a closure.
 * We don't care what comes back because it will be reflected into the constructor and the JVM will sort it out.
 */
internal typealias FieldReader = (resultSet: ResultSet) -> Any?

/**
 * Everything needed to create a T from a ResultSet.
 */
@PublishedApi
internal data class RecordReader<T>(val ctor: Constructor<T>, val fields: List<FieldReader>)

/**
 * Provide a ReadRecord for a type T.
 */
@PublishedApi
internal fun <T : Any> makeReadRecord(kClass: KClass<T>): RecordReader<T> {

    val ctor: Constructor<T> = kClass.primaryConstructor!!.javaConstructor!!
    require(ctor.trySetAccessible()) { "Primary constructor of ${kClass.jvmName} is inaccessible." }
    val params = ctor.parameters!!
    val fields: List<FieldReader> = params.withIndex().map { ctorParam ->
        val columnIndex = 1 + ctorParam.index
        when (ctorParam.value.type) {
            java.lang.Character::class.java -> { rs ->
                rs.getString(columnIndex).let {
////                        // Fudge for Oracle
////                        if (it.isEmpty()) null
//                        else it[0]
                    it[0]
                }.let {
                    if (rs.wasNull()) null else it
                }
            }

            java.lang.String::class.java -> { rs ->
                rs.getString(columnIndex).let {
                    if (rs.wasNull()) null else it
                }
            }

            java.lang.Integer::class.java -> { rs ->
                rs.getInt(columnIndex).let {
                    if (rs.wasNull()) null else it
                }
            }

            java.lang.Double::class.java -> { rs ->
                rs.getDouble(columnIndex).let {
                    if (rs.wasNull()) null else it
                }
            }
            // Punt.
            else -> { rs ->
                rs.getObject(columnIndex).let {
                    if (rs.wasNull()) null else it
                }
            }
        }
    }
    return RecordReader(ctor, fields)
}

// Nulls are a separate case.
typealias Coercion = (Any) -> Any

///**
// * Maps DB type to field type to coercion.
// */
//private val fromSQL = mapOf<String, Map<String, Coercion>>(
//    "boolean" to mapOf(
//        BigDecimal::class.java.canonicalName to { t ->
//            (t as BigDecimal).let {
//                0 != it.compareTo(BigDecimal.ZERO)
//            }
//        }
//    )
//)
//
//private fun coerce(param: Parameter, arg: Any?): Any? =
//    when (arg) {
//        null -> null
//        else -> {
//            val sqlType = param.type.canonicalName
//            val fieldType = arg::class.java.canonicalName
//            fromSQL[sqlType]?.get(fieldType)?.let { it(arg) } ?: arg
//        }
//    }
//
private val coerce = object {
    private val fromSQL = mapOf<String, Map<String, Coercion>>(
        "boolean" to mapOf(
            BigDecimal::class.java.canonicalName to { t ->
                (t as BigDecimal).let {
                    0 != it.compareTo(BigDecimal.ZERO)
                }
            }
        )
    )

    /**
     * Coerce a value to its corresponding parameter type.
     *
     * @param param Description of the parameter of the object constructor.
     * @param arg The value to be given to the constructor.
     */
    fun param(param: Parameter, arg: Any?): Any? =
        when (arg) {
            null -> null
            else -> {
                val sqlType = param.type.canonicalName
                val fieldType = arg::class.java.canonicalName
                fromSQL[sqlType]?.get(fieldType)?.let { it(arg) } ?: arg
            }
        }

    /**
     * Coerce a bunch of values to their corresponding parameter types.
     */
    fun params(parameters: Array<Parameter>, args: Collection<Any?>): Array<Any?> {
        require(parameters.size == args.size)
        return parameters.zip(args).map { pair ->
            param(pair.first, pair.second)
        }.toTypedArray()
    }
}

@PublishedApi
@Throws(DBException::class)
internal fun <T : Any> ResultSet.readRecord(recordReader: RecordReader<T>): T {
    val args: List<Any?> = recordReader.fields.map { it(this) }
    val parameters: Array<Parameter> = recordReader.ctor.parameters
    require(args.size == parameters.size)
    val coerced = coerce.params(parameters, args)
    try {
        return recordReader.ctor.newInstance(* coerced)
    } catch (e: java.lang.IllegalArgumentException) {
        throw DBException(
            """Could not match constructor ${recordReader.ctor.name} with arguments: 
              |${
                parameters.zip(coerced).withIndex().joinToString("") { q ->
                    val p: Parameter = q.value.first
                    val v: Any? = q.value.second
                    "\n   ${p.name} : ${p.type} = ${if (null == v) "null" else "[${v::class.qualifiedName}] $v"} " +
                            "<< ${coerced[q.index].let { "$it as ${it?.javaClass?.canonicalName ?: "NULL"}" }} >>"
                }
            }""".trimMargin(), e
        )
    } catch (e: Throwable) {
        throw RuntimeException("Failed to read record: ${args.joinToString()}", e)
    }
}

/**
 * Runs a mutating statement (CREATE, INSERT, UPDATE, or DELETE),
 * interpolating the params in order to the prepared statement.
 *
 * Note that insert gets special treatment, below, as well.
 */
fun Connection.update(sql: String, vararg params: SQLParam): Int =
    prepareStatement(sql).use { it.setParams(* params).executeUpdate() }

fun Connection.update(sql: String, params: (Params.() -> Unit)): Int =
    prepareStatement(sql).use {
        it.setParams(Params().also { p -> p.params() }).executeUpdate()
    }

/**
 * Encapsulates the marshaling of a value from a record to a position in a prepared statement.
 * @param prop extracts the value from a record.
 * @param columnType used when writing a null.
 */
@PublishedApi
internal abstract class FieldWriter<R, T : Any>(private val prop: KProperty1<R, *>, private val columnType: Int) {

    /**
     * Strategy for writing a type appropriate value or a null.
     */
    fun write(record: R, ps: PreparedStatement, pos: Int) {
        val value = prop.get(record)
        @Suppress("UNCHECKED_CAST")
        if (value == null)
            ps.setNull(pos, columnType)
        else
            writeValue(ps, pos, value as T)
    }

    /**
     * Implement this by setting the value into the prepared statement in a type appropriate way,
     * i.e. by casting the value.
     */
    protected abstract fun writeValue(ps: PreparedStatement, pos: Int, value: T)
}

/**
 * Inserts a collection of records into a table.
 */
@Throws(SQLException::class, DBException::class)
inline fun <reified T : Any> Connection.insert(entity: Entity, record: T): Int =
    insert(T::class, entity.tableName, entity.fieldMap, record)

/**
 * Non-inlined version of insert.
 */
@Throws(SQLException::class, DBException::class)
@PublishedApi
internal fun <T : Any> Connection.insert(
    kClass: KClass<T>, table: String, columns: Collection<Pair<String, String>>, record: T
): Int {
    require(columns.isNotEmpty()) { "No columns are declared." }

    val props: Collection<KProperty1<T, *>> = kClass.declaredMemberProperties
    val propNames = props.toMapStrict { it.name }

    val width = columns.size
    require(width == props.size) {
        "Size of declared fields (${columns.joinToString()}) does not match the record size (${props.size}) in ${kClass.qualifiedName}"
    }

    val writes: List<FieldWriter<T, out Any>> = columns.map { column ->
        val prop: KProperty1<T, *> = propNames[column.second]
            ?: throw RuntimeException("Unknown property ${column.second} on ${kClass.qualifiedName}")
        when (prop.returnType.toString()) {
            "kotlin.String" ->
                object : FieldWriter<T, String>(prop, Types.VARCHAR) {
                    override fun writeValue(ps: PreparedStatement, pos: Int, value: String) =
                        ps.setString(pos, value)
                }

            java.lang.Integer::class.java.canonicalName ->
                object : FieldWriter<T, Int>(prop, Types.INTEGER) {
                    override fun writeValue(ps: PreparedStatement, pos: Int, value: Int) =
                        ps.setInt(pos, value)
                }

            java.lang.Double::class.java.canonicalName ->
                object : FieldWriter<T, Double>(prop, Types.DOUBLE) {
                    override fun writeValue(ps: PreparedStatement, pos: Int, value: Double) =
                        ps.setDouble(pos, value)
                }

            else ->
                object : FieldWriter<T, Any>(prop, Types.JAVA_OBJECT) {
                    override fun writeValue(ps: PreparedStatement, pos: Int, value: Any) =
                        ps.setObject(pos, value)
                }
        }
    }

    val projection = columns.joinToString { it.first }
    val stmt = "INSERT INTO $table ($projection) VALUES (${inList(columns)})"
    val ps = prepareStatement(stmt)

    writes.withIndex().forEach { w ->
        w.value.write(record, ps, 1 + w.index)
    }

    return ps.executeUpdate()
}

/**
 * For SELECT COUNT statements that return a single record with a single integer column.
 */
@Throws(SQLException::class, DBException::class)
fun Connection.count(sql: String, block: Params.() -> Unit): Int =
    prepareStatement(sql).use { ps ->
        countImpl(ps, Params().also { it.block() })
    }

/**
 * For SELECT COUNT statements that return a single record with a single integer column.
 */
@Throws(SQLException::class, DBException::class)
fun Connection.count(sql: String): Int =
    prepareStatement(sql).use { ps ->
        countImpl(ps, Params())
    }

private fun countImpl(ps: PreparedStatement, it: Params): Int {
    ps.setParams(it).executeQuery().use { rs ->
        if (1 != rs.metaData.columnCount) throw DBException("Single column expected for COUNT.")
        if (!rs.next()) throw DBException("No data in record set.")
        val count = rs.getInt(1)
        if (rs.next()) throw DBException("Non-unique record set.")
        return count
    }
}

/**
 * Marshals the zero or one results from the query into an optional T.
 * If more than one record is returned then a `DBException` will be thrown.
 */
@Throws(SQLException::class, DBException::class)
inline fun <reified T : Any> Connection.unique(sql: String, params: (Params.() -> Unit)): T? =
    unique(T::class, sql, Params().also { it.params() }.array())

@Throws(SQLException::class, DBException::class)
inline fun <reified T : Any> Connection.unique(sql: String): T? =
    unique(T::class, sql, arrayOf())

/**
 * Non-inlined version of unique.
 */
@Throws(SQLException::class, DBException::class)
@PublishedApi
internal fun <T : Any> Connection.unique(
    kClass: KClass<T>, sql: String, params: Array<out SQLParam>
): T? {
    prepareStatement(sql).use { ps ->
        ps.setParams(* params).executeQuery().use { rs ->
            if (!rs.next()) return null
            val recordClass = makeReadRecord(kClass)
            val record = rs.readRecord(recordClass)
            if (rs.next()) throw DBException("Non-unique record set.")
            return record
        }
    }
}

@Throws(SQLException::class, DBException::class)
inline fun <reified T : Any> Connection.list(sql: String, params: (Params.() -> Unit)): List<T> =
    list(T::class, sql, Params().also { p -> p.params() }.array())

@Throws(SQLException::class, DBException::class)
inline fun <reified T : Any> Connection.list(sql: String): List<T> =
    list(T::class, sql, arrayOf())

/**
 * Marshals the result from a query into a list of T.
 * Non-inlined implementation of list.
 */
@Throws(SQLException::class, DBException::class)
@PublishedApi
internal fun <T : Any> Connection.list(
    kClass: KClass<T>, sql: String, params: Array<out SQLParam>
): List<T> {
    prepareStatement(sql).use { ps ->
        ps.setParams(* params).executeQuery().use { rs ->
            val recordClass = makeReadRecord(kClass)
            return LinkedList<T>().also { records ->
                while (rs.next()) {
                    records.add(rs.readRecord(recordClass))
                }
            }
        }
    }
}

