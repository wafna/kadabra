package wafna.kadabra

import java.math.BigDecimal
import java.sql.*
import java.util.*
import kotlin.reflect.*
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.javaConstructor
import kotlin.reflect.jvm.jvmName

/**
 * Converts a collection to a map by applying a key generating function to each element.
 * Fails on key collisions.
 */
private fun <T : Any, R : Any> Collection<T>.toMapStrict(key: (T) -> R): Map<R, T> =
    fold(TreeMap<R, T>()) { map, elem ->
        val k = key(elem)
        if (map.containsKey(k))
            throw DBException("Duplicate key: $k")
        map[k] = elem
        map
    }

class DBException(message: String, cause: Throwable? = null) : Exception(message, cause)

/**
 * Information needed to map an object into and out of a database.
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
val Timestamp.sql: SQLParam
    get() = { ps, position -> ps.setTimestamp(position, this) }
val Any.sql: SQLParam
    get() = { ps, position -> ps.setObject(position, this) }

/**
 * Collection of parameters to prepared statements.
 */
class Params {
    private val params = mutableListOf<SQLParam>()

    fun array(): Array<SQLParam> = params.toTypedArray()

    fun add(vararg ps: SQLParam) = ps.forEach { params.add(it) }

    fun addInt(p: Int) = params.add(p.sql)
    fun addDouble(p: Double) = params.add(p.sql)
    fun addStrings(vararg ps: String) = ps.forEach { params.add(it.sql) }
    fun addTimestamp(p: Timestamp) = params.add(p.sql)
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
//private typealias FieldReader = (resultSet: ResultSet) -> Any?

internal abstract class FieldReader(private val columnIndex: Int) {
    fun read(resultSet: ResultSet): Any? {
        try {
            return readField(resultSet)?.let {
                if (resultSet.wasNull()) null else it
            }
        } catch (e: Throwable) {
            throw DBException("Cannot get parameter at position $columnIndex.", e)
        }
    }

    protected abstract fun readField(resultSet: ResultSet): Any?
}

/**
 * Everything needed to create a T from a ResultSet.
 */
internal data class RecordReader<T>(val ctor: KFunction<T>, val fields: List<FieldReader>)

/**
 * Provide a ReadRecord for a type T.
 */
private fun <T : Any> makeRecordReader(kClass: KClass<T>): RecordReader<T> {

    val ctor: KFunction<T> = kClass.primaryConstructor!!
    require(ctor.javaConstructor!!.trySetAccessible()) { "Primary constructor of ${kClass.jvmName} is inaccessible." }
    val fields: List<FieldReader> = ctor.parameters.withIndex().map { ctorParam ->
        makeFieldReader(ctorParam.value, 1 + ctorParam.index)
    }
    return RecordReader(ctor, fields)
}

private fun makeFieldReader(
    param: KParameter,
    columnIndex: Int
): FieldReader {
    return when (param) {
        String::class -> object : FieldReader(columnIndex) {
            override fun readField(resultSet: ResultSet): Any? =
                resultSet.getString(columnIndex)
        }

        Integer::class -> object : FieldReader(columnIndex) {
            override fun readField(resultSet: ResultSet): Any =
                resultSet.getInt(columnIndex)
        }

        Long::class -> object : FieldReader(columnIndex) {
            override fun readField(resultSet: ResultSet): Any =
                resultSet.getLong(columnIndex)
        }

        Double::class -> object : FieldReader(columnIndex) {
            override fun readField(resultSet: ResultSet): Any =
                resultSet.getDouble(columnIndex)
        }

        BigDecimal::class -> object : FieldReader(columnIndex) {
            override fun readField(resultSet: ResultSet): Any? =
                resultSet.getBigDecimal(columnIndex)
        }

        Timestamp::class -> object : FieldReader(columnIndex) {
            override fun readField(resultSet: ResultSet): Any? =
                resultSet.getTimestamp(columnIndex)
        }

        else -> object : FieldReader(columnIndex) {
            override fun readField(resultSet: ResultSet): Any? =
                resultSet.getObject(columnIndex)
        }
    }
}

@PublishedApi
@Throws(DBException::class)
internal fun <T : Any> ResultSet.readRecord(recordReader: RecordReader<T>): T {
    val args: List<Any?> = recordReader.fields.map { it.read(this) }
    try {
        return recordReader.ctor.call(* args.toTypedArray())
    } catch (e: java.lang.IllegalArgumentException) {
        throw DBException("Could not match constructor ${recordReader.ctor.name} with arguments", e)
    } catch (e: Throwable) {
        throw DBException("Failed to read record: ${args.joinToString()}", e)
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
        if (value == null) {
            try {
                ps.setNull(pos, columnType)
            } catch (e: Throwable) {
                throw DBException("Cannot set NULL with column type $columnType at position $pos.")
            }
        } else {
            @Suppress("UNCHECKED_CAST")
            writeValue(ps, pos, value as T)
        }
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
internal fun <R : Any> Connection.insert(
    kClass: KClass<R>, table: String, columns: Collection<Pair<String, String>>, record: R
): Int {
    require(columns.isNotEmpty()) { "No columns are declared." }

    val props: Collection<KProperty1<R, *>> = kClass.declaredMemberProperties
    val propNames = props.toMapStrict { it.name }

    val width = columns.size
    require(width == props.size) {
        "Size of declared fields (${columns.joinToString()}) does not match the record size (${props.size}) in ${kClass.qualifiedName}"
    }

    val writes: List<FieldWriter<R, out Any>> = columns.map { column ->
        val prop: KProperty1<R, *> = propNames[column.second]
            ?: throw DBException("Unknown property ${column.second} on ${kClass.qualifiedName}")
        when (prop.returnType.classifier) {
            String::class -> object : FieldWriter<R, String>(prop, Types.VARCHAR) {
                override fun writeValue(ps: PreparedStatement, pos: Int, value: String) =
                    ps.setString(pos, value)
            }

            Integer::class -> object : FieldWriter<R, Int>(prop, Types.INTEGER) {
                override fun writeValue(ps: PreparedStatement, pos: Int, value: Int) =
                    ps.setInt(pos, value)
            }

            Long::class -> object : FieldWriter<R, Long>(prop, Types.INTEGER) {
                override fun writeValue(ps: PreparedStatement, pos: Int, value: Long) =
                    ps.setLong(pos, value)
            }

            Double::class -> object : FieldWriter<R, Double>(prop, Types.DOUBLE) {
                override fun writeValue(ps: PreparedStatement, pos: Int, value: Double) =
                    ps.setDouble(pos, value)
            }

            BigDecimal::class -> object : FieldWriter<R, BigDecimal>(prop, Types.DOUBLE) {
                override fun writeValue(ps: PreparedStatement, pos: Int, value: BigDecimal) =
                    ps.setBigDecimal(pos, value)
            }

            Timestamp::class -> object : FieldWriter<R, Timestamp>(prop, Types.DOUBLE) {
                override fun writeValue(ps: PreparedStatement, pos: Int, value: Timestamp) =
                    ps.setTimestamp(pos, value)
            }

            else -> object : FieldWriter<R, Any>(prop, Types.JAVA_OBJECT) {
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
            val recordClass = makeRecordReader(kClass)
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
            val recordClass = makeRecordReader(kClass)
            return LinkedList<T>().also { records ->
                while (rs.next()) {
                    records.add(rs.readRecord(recordClass))
                }
            }
        }
    }
}

