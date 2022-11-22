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
    init {
        require(tableName.isNotEmpty())
        require(columnNames.size == fieldNames.size)
    }

    /**
     * All the columns (in order), qualified with `prefix`, in a comma separated list.
     * Useful for SELECT and INSERT.
     */
    fun projection(prefix: String = tableName) = columnNames.qualify(prefix).project()
    val fieldMap = columnNames.zip(fieldNames)
}

fun inList(size: Int): String = List(size) { "?" }.joinToString()
fun inList(items: Collection<Any?>): String = inList(items.size)

fun List<String>.qualify(tableName: String): List<String> =
    map { "$tableName.$it" }

fun List<String>.project(): String = joinToString(", ")

/**
 * Collection of parameters to prepared statements.
 * Their positions in the prepared statement correspond to the order in which they were added.
 */
class SQLParams(private val ps: PreparedStatement) {

    private var index = 0
    private fun next(): Int = ++index

    fun add(vararg ps: Int) = ps.forEach {
        this.ps.setInt(next(), it)
    }

    fun add(vararg ps: Long) = ps.forEach {
        this.ps.setLong(next(), it)
    }

    fun add(vararg ps: Double) = ps.forEach {
        this.ps.setDouble(next(), it)
    }

    fun add(vararg ps: String) = ps.forEach {
        this.ps.setString(next(), it)
    }

    fun add(vararg ps: Timestamp) = ps.forEach {
        this.ps.setTimestamp(next(), it)
    }

    fun add(vararg ps: BigDecimal) = ps.forEach {
        this.ps.setBigDecimal(next(), it)
    }

    fun add(vararg ps: UUID) = ps.forEach {
        this.ps.setObject(next(), it)
    }

    fun addObject(vararg ps: Any) = ps.forEach {
        this.ps.setObject(next(), it)
    }
}

/**
 * Gets a value from a result set at a position.
 * We don't care what comes back because it will be reflected into the constructor and the JVM will sort it out.
 */
internal abstract class FieldReader(private val columnIndex: Int) {
    fun read(resultSet: ResultSet): Any? {
        try {
            return readField(resultSet)?.let {
                // Sometimes we get a default value (viz. primitives, unboxed values) for null.
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
    val fields: List<FieldReader> = ctor.parameters.withIndex().map {
        makeFieldReader(it.value, 1 + it.index)
    }
    return RecordReader(ctor, fields)
}

private fun makeFieldReader(param: KParameter, columnIndex: Int): FieldReader {
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
        throw DBException(
            "Could not match constructor ${recordReader.ctor.returnType} with arguments" +
                    recordReader.ctor.parameters.zip(args)
                        .joinToString { "\n   ${it.first.name}: ${it.first.type} <- ${it.second?.let { v -> "${v::class.qualifiedName}" } ?: "null"}" },
            e
        )
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
fun Connection.update(sql: String): Int =
    prepareStatement(sql).use { it.executeUpdate() }

fun Connection.update(sql: String, params: (SQLParams.() -> Unit)): Int =
    prepareStatement(sql).use {
        params(SQLParams(it))
        it.executeUpdate()
    }

/**
 * Encapsulates the marshaling of a value from a record to a position in a prepared statement.
 * This is for INSERT.
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
fun <R : Any> Connection.insert(entity: Entity, record: R): Int {
    val kClass: KClass<R> = record.javaClass.kotlin
    val table = entity.tableName
    val columns = entity.fieldMap
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

            BigDecimal::class -> object : FieldWriter<R, BigDecimal>(prop, Types.NUMERIC) {
                override fun writeValue(ps: PreparedStatement, pos: Int, value: BigDecimal) =
                    ps.setBigDecimal(pos, value)
            }

            Timestamp::class -> object : FieldWriter<R, Timestamp>(prop, Types.TIMESTAMP) {
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
fun Connection.count(sql: String, configure: SQLParams.() -> Unit): Int =
    prepareStatement(sql).use { ps ->
        SQLParams(ps).configure()
        countImpl(ps)
    }

/**
 * For SELECT COUNT statements that return a single record with a single integer column.
 */
@Throws(SQLException::class, DBException::class)
fun Connection.count(sql: String): Int =
    prepareStatement(sql).use { ps ->
        countImpl(ps)
    }

private fun countImpl(ps: PreparedStatement): Int {
    ps.executeQuery().use { rs ->
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
inline fun <reified T : Any> Connection.unique(sql: String, params: (SQLParams.() -> Unit)): T? =
    prepareStatement(sql).use { ps ->
        SQLParams(ps).params()
        unique(T::class, ps)
    }

/**
 * Marshals the zero or one results from the query into an optional T.
 * If more than one record is returned then a `DBException` will be thrown.
 */
@Throws(SQLException::class, DBException::class)
inline fun <reified T : Any> Connection.unique(sql: String): T? =
    prepareStatement(sql).use { ps ->
        unique(T::class, ps)
    }

/**
 * Non-inlined implementation of unique.
 */
@Throws(SQLException::class, DBException::class)
@PublishedApi
internal fun <T : Any> unique(kClass: KClass<T>, ps: PreparedStatement): T? {
    ps.executeQuery().use { rs ->
        if (!rs.next()) return null
        val recordClass = makeRecordReader(kClass)
        val record = rs.readRecord(recordClass)
        if (rs.next()) throw DBException("Non-unique record set.")
        return record
    }
}

/**
 * Marshals the result from a query into a list of T.
 */
@Throws(SQLException::class, DBException::class)
inline fun <reified T : Any> Connection.list(sql: String, params: (SQLParams.() -> Unit)): List<T> =
    prepareStatement(sql).use { ps ->
        SQLParams(ps).params()
        list(T::class, ps)
    }

/**
 * Marshals the result from a query into a list of T.
 */
@Throws(SQLException::class, DBException::class)
inline fun <reified T : Any> Connection.list(sql: String): List<T> =
    prepareStatement(sql).use { ps ->
        list(T::class, ps)
    }

/**
 * Non-inlined implementation of list.
 */
@Throws(SQLException::class, DBException::class)
@PublishedApi
internal fun <T : Any> list(kClass: KClass<T>, ps: PreparedStatement): List<T> {
    ps.executeQuery().use { rs ->
        val recordClass = makeRecordReader(kClass)
        return LinkedList<T>().also { records ->
            while (rs.next()) {
                records.add(rs.readRecord(recordClass))
            }
        }
    }
}

