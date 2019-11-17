/*
 * Copyright (c) 2018. Martin Heinrich - All Rights Reserved
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 */

package org.daiv.reflection.persister

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.daiv.reflection.common.*
import org.daiv.reflection.database.DatabaseHandler
import org.daiv.reflection.database.DatabaseInterface
import org.daiv.reflection.isPrimitiveOrWrapperOrString
import org.daiv.reflection.plain.RequestBuilder
import org.daiv.reflection.plain.SimpleReadObject
import org.daiv.reflection.plain.readPlainMapper
import org.daiv.reflection.read.InternalRPD
import org.daiv.reflection.read.KeyType
import org.daiv.reflection.read.ReadFieldValue
import org.daiv.reflection.read.ReadPersisterData
import org.daiv.util.DefaultRegisterer
import org.daiv.util.Registerer
import org.slf4j.MarkerFactory
import java.sql.ResultSet
import java.sql.SQLException
import kotlin.reflect.KClass
import kotlin.reflect.full.cast

internal val dbOpen = MarkerFactory.getMarker("DB_OPEN")
internal val dbClose = MarkerFactory.getMarker("DB_CLOSE")
private val persisterMarker = MarkerFactory.getMarker("Persister")

/**
 * @author Martin Heinrich
 */
class Persister(private val databaseInterface: DatabaseInterface,
                val persisterPreference: PersisterPreference = defaultPersisterPreference(),
                private val registerer: DefaultRegisterer<DBChangeListener> = DefaultRegisterer()) :
        Registerer<DBChangeListener> by registerer {
    constructor(dbPath: String, persisterPreference: PersisterPreference = defaultPersisterPreference()) :
            this(DatabaseHandler(dbPath), persisterPreference)

    fun close() = databaseInterface.close()

    val logger = KotlinLogging.logger {}
    //    val logger = KotlinLogging.logger("Persister. ${databaseInterface.path}")
    val dbMarkerRead = MarkerFactory.getDetachedMarker("READ")
    val dbMarkerWrite = MarkerFactory.getDetachedMarker("WRITE")
    val dbMarkerCreate = MarkerFactory.getDetachedMarker("CREATE")

    init {
        val dbMarker = MarkerFactory.getMarker(databaseInterface.path)
        dbMarkerRead.add(dbMarker)
        dbMarkerRead.add(persisterMarker)
        dbMarkerWrite.add(dbMarker)
        dbMarkerWrite.add(persisterMarker)
        dbMarkerCreate.add(dbMarkerWrite)
    }

    fun delete() = databaseInterface.delete()

    private fun event() {
        registerer.forEach(DBChangeListener::onChange)
    }

    private val internalReadCache = ReadCache(persisterPreference)

    private fun readCache() = if (persisterPreference.useCache) internalReadCache else ReadCache(persisterPreference)

    internal fun justPersist(tableName: String, readPersisterData: InternalRPD<*, *>) {
        write("$createTable $tableName ${readPersisterData.createTable()}")
    }

    internal fun <T : Any> read(query: String, func: (ResultSet) -> T): T {
        try {

            logger.trace(dbMarkerRead, query)
            val statement = databaseInterface.statement
            val result = statement.executeQuery(query)
            val ret = func(result)
            result.close()
            statement.close()
            return ret
        } catch (e: SQLException) {
            throw RuntimeException("query: $query", e)
        }
    }

    internal fun write(query: String) {
        try {
            if (query.startsWith("CREATE")) {
                logger.trace(dbMarkerCreate, query)
            } else {
                logger.trace(dbMarkerWrite, query)
            }
            val statement = databaseInterface.statement
            statement.execute(query)
            statement.close()
        } catch (e: SQLException) {
            throw RuntimeException("query: $query", e)
        }
    }

    private fun getTableName(tableName: String, clazz: KClass<*>) = if (tableName == "") clazz.tableName() else tableName

    internal interface InternalTable : PersisterListener {
        val readPersisterData: InternalRPD<out Any, Any>
        val innerTableName: String
        val tableName: String
        val _tableName: String
        val persister: Persister

        fun dropTable(tableName: String = this.tableName) {
            readPersisterData.dropHelper()
            onlyDropMaster(tableName)
        }

        fun onlyDropMaster(tableName: String = this.tableName) {
            persister.write("DROP TABLE $tableName;")
        }

        fun rename(oldTableName: String, newTableName: String) {
            persister.write("ALTER TABLE $oldTableName RENAME TO `$newTableName`")
        }

        fun namesToMap() = readPersisterData.names()

        fun copyData(newVariables: Map<String, String>, oldTableName: String, newTableName: String, request: (String) -> String) {
            val command = this.readPersisterData.copyTable(this.namesToMap() + newVariables, request)
            persister.write("INSERT INTO $newTableName $command from $oldTableName;")
        }

        fun persist() {
            persistWithName(tableName)
        }

        fun persistWithName(tableName: String = this.tableName,
                            checkName: Set<String> = emptySet(),
                            _tableName: String = this._tableName): Set<String> {
            persister.justPersist(tableName, readPersisterData)
            val ret = readPersisterData.createTableForeign(checkName + _tableName)
            persister.event()
            return ret
        }

        fun fromWhere(fieldName: String, id: Any, sep: String): String {
            try {
                val x = " FROM $tableName ${readPersisterData.fromWhere(fieldName, id, sep)}"
                return x
            } catch (t: Throwable) {
                throw t
            }
        }

        fun readIntern(fnEqualsValue: String, readCache: ReadCache, orderOrder: String = ""): List<List<ReadFieldValue>> {
//            val req = "SELECT $selectHeader FROM $tableName $join ${whereClause(fieldName, id, and)};"
            val req = "SELECT * FROM $tableName WHERE $fnEqualsValue $orderOrder;"
            return persister.read(req) { it.getList { readPersisterData.readWOObject(ReadValue(this), readCache) } }
        }

        fun clear() {
            persister.write("DELETE from $tableName;")
            readPersisterData.clearLists()
            onClear(tableName)

            persister.event()
        }

        fun deleteBy(fieldName: String, id: List<Any>) {
            try {
                onDelete(tableName, id)
                persister.write("DELETE ${fromWhere(fieldName, id, comma)};")
                persister.event()
            } catch (e: Throwable) {
                throw e
            }
        }

    }

    internal inner class HelperTable constructor(val fields: List<FieldData<Any, Any, Any, Any>>,
                                                 val innerTableNameGetter: () -> String,
                                                 val tableNameGetter: () -> String,
                                                 numberOfKeyFields: Int = 1) :
            InternalTable, PersisterListener by internalReadCache {

        override val readPersisterData: InternalRPD<out Any, Any> = object : InternalRPD<Any, Any> {
            override val fields: List<FieldData<Any, Any, Any, Any>> = this@HelperTable.fields
            override val key: KeyType = KeyType(fields.take(numberOfKeyFields))
        }

        override val innerTableName: String
            get() = innerTableNameGetter()
        override val tableName: String
            get() = tableNameGetter()
        override val _tableName: String
            get() = tableName
        override val persister: Persister = this@Persister
    }

    private fun <R : Any> createPersisterProvider(clazz: KClass<R>,
                                                  tableName: String,
                                                  tableNames: Map<KClass<out Any>, String>,
                                                  tableNamePrefix: String?): PersisterProvider {
        val persisterProvider = PersisterProviderImpl(this@Persister, tableNames, tableNamePrefix)
        persisterProvider[clazz] = getTableName(tableName, clazz)
        return persisterProvider
    }

    inner class Table<R : Any> internal constructor(override val readPersisterData: ReadPersisterData<R, Any>,
                                                    val clazz: KClass<R>) :
            InternalTable, Registerer<DBChangeListener> by registerer, PersisterListener by internalReadCache {
        override val innerTableName: String
            get() = readPersisterData.persisterProvider.innerTableName(clazz)
        override val _tableName
            get() = readPersisterData.persisterProvider[clazz]
        /**
         * same as [_tableName] with backticks
         */
        override val tableName
            get() = "`$_tableName`"
        override val persister = this@Persister

        private val persisterProvider
            get() = readPersisterData.persisterProvider


        constructor(clazz: KClass<R>,
                    tableName: String = "",
                    tableNames: Map<KClass<out Any>, String> = mapOf(),
                    tableNamePrefix: String? = null)
                : this(clazz, createPersisterProvider(clazz, tableName, tableNames, tableNamePrefix))

        internal constructor(clazz: KClass<R>, persisterProvider: PersisterProvider)
                : this(ReadPersisterData(clazz, this@Persister, persisterProvider), clazz)

        private val registerer: DefaultRegisterer<DBChangeListener> = DefaultRegisterer()

        private val selectHeader by lazy { readPersisterData.underscoreName() }
        private val selectKeyHeader by lazy { readPersisterData.keyName() }

        private val idName = readPersisterData.getIdName()

        internal fun tableEvent() {
            registerer.forEach(DBChangeListener::onChange)
            event()
        }

        fun rename(newTableName: String): Table<R> {
            this.rename(this.tableName, newTableName)
            readPersisterData.persisterProvider[clazz] = newTableName
            return this
        }

        private fun <T : Any, S : InternalTable> change(next: Table<T>,
                                                        newVariables: Map<String, String> = mapOf(),
                                                        creator: (String) -> S): S {
            persister.justPersist(next._tableName, next.readPersisterData)
//            next.persist()
            next.copyData(newVariables, tableName, next.tableName) { it }
            onlyDropMaster()
            next.rename(next.tableName, _tableName)
            persisterProvider.rename(next.clazz, innerTableName)
            return creator(_tableName)
        }

        fun <T : Any> change(clazz: KClass<T>, newVariables: Map<String, String> = mapOf()): Table<T> {
            val tName = "${innerTableName}_temp"
            persisterProvider.rename(clazz, tName)
            val next = persister.Table(clazz, persisterProvider)
            return change(next, newVariables) { Table(next.readPersisterData, clazz) }
        }

        private fun toKeyNames(helperTableName: String) = readPersisterData.key.fields.map { "$tableName.${it.key()} = $helperTableName.${it.key()}" }
                .joinToString(" $and ")

        fun copyHelperTable(map: Map<String, Map<String, String>>, oldTable: Table<out Any>? = null): Table<R> {
            val request: (String, String) -> String = { helperTableName, variable ->
                "(select $variable from $tableName where ${oldTable?.toKeyNames(helperTableName)})"
            }
            readPersisterData.copyHelperTable(map, request)
            return this
        }

        internal fun read(fieldName: String, id: Any, readCache: ReadCache, orderOrder: String = ""): List<R> {
            val req = "SELECT * ${fromWhere(fieldName, id, and)} $orderOrder;"
            return this@Persister.read(req) { it.getList { readPersisterData.evaluate(ReadValue(this), readCache) } }
        }

        fun read(fieldName: String, id: Any, orderOrder: String = ""): List<R> {
            return read(fieldName, id, readCache(), orderOrder).map { it }
        }

        fun readOrdered(fieldName: String, id: Any): List<R> {
            return read(fieldName, id, readCache(), orderOrder = "ORDER BY ${readPersisterData.keyName()}").map { it }
        }

        private fun Any.toList(): List<Any> {
            val req = if (this is List<*>) {
                this as List<Any>
            } else
                listOf(this)
            if (!readPersisterData.key.isKeyType(req)) {
                val asList = listOf(req)
                if (!readPersisterData.key.isKeyType(asList)) {
                    throw RuntimeException("$this is no key for table $_tableName")
                }
                return asList
            }
            return req
        }

        private fun List<Any>.toHashCodeX(): List<Any> {
            return if (readPersisterData.key.isAuto()) {
                listOf(readPersisterData.key.plainHashCodeXIfAutoKey(this))
            } else {
                this
            }
        }

        fun read(id: Any): R? {
            return read(idName, id.toList().toHashCodeX(), readCache()).firstOrNull()
        }

        fun readMultiple(id: List<Any>): R? {
            return read(idName, id.toHashCodeX(), readCache()).firstOrNull()
        }

        internal fun innerReadMultipleUseHashCode(id: List<Any>, readCache: ReadCache): R? {
            return read(idName, id, readCache).firstOrNull()
        }

        fun readMultipleUseHashCode(id: List<Any>): R? {
            return innerReadMultipleUseHashCode(id, readCache())
        }

//        fun readMultiple(vararg id: Any): R? {
//            return readMultiple(id.asList())
//        }

//        fun read(id: List<Any>): R? {
//            return read(idName, id).firstOrNull()
//        }

        fun insert(o: R) {
            insert(listOf(o))
        }

        private fun innerCachePreference() = InsertCachePreference(false)
        private fun CoroutineScope.actors(requestScope: CoroutineScope,
                                          completable: Boolean) = persisterProvider.tableNamesIncludingPrefix().map {
            val actorHandler = ActorHandler(this, requestScope, readCache().readTableCache(it))
            it to if (!completable) InsertActor(actorHandler) else InsertCompletableActor(actorHandler)
        }
                .toMap()

        private fun tableHandlers() = persisterProvider.tableNamesIncludingPrefix().map {
            it to SequentialTableHandler(readCache().readTableCache(it))
        }
                .toMap()


        inner class InsertCacheParallel(val coroutineScope: CoroutineScope, val requestScope: CoroutineScope, completable: Boolean,
                                        val insertCachePreference: InsertCachePreference = innerCachePreference()) :
                InsertCache<R> by InsertCacheHandler(InsertMap(persister,
                                                               insertCachePreference,
                                                               coroutineScope.actors(requestScope, completable),
                                                               readCache()), readPersisterData, this, true)

        inner class InsertCacheSeriell(val insertCachePreference: InsertCachePreference = innerCachePreference()) :
                InsertCache<R> by InsertCacheHandler(InsertMap(persister, insertCachePreference, tableHandlers(), readCache()),
                                                     readPersisterData, this, false)

        fun insert(o: List<R>, innerCachePreference: InsertCachePreference = innerCachePreference()) {
            if (o.isEmpty()) {
                return
            }

            runBlocking {
                val map = InsertMap(persister, innerCachePreference, tableHandlers(), readCache())
                readPersisterData.putInsertRequests(_tableName, map, o)
                map.insertAll()
            }
            tableEvent()
        }

        fun exists(fieldName: String, id: Any): Boolean {
            return this@Persister.read("SELECT EXISTS( SELECT $selectHeader ${fromWhere(fieldName, id, comma)});") { it.getInt(1) != 0 }
        }

        fun exists(id: Any): Boolean {
            return exists(idName, id.toList())
        }

        private fun innerDelete(fieldName: String, id: Any) {
            try {
                val list = read(fieldName, id)
                this@Persister.write("DELETE ${fromWhere(fieldName, id, and)};")
                list.forEach { readPersisterData.deleteLists(readPersisterData.keySimpleType(it).toList()) }
                tableEvent()
            } catch (e: Throwable) {
                throw e
            }
        }

        fun delete(fieldName: String, id: Any) {
            onDelete(tableName)
            innerDelete(fieldName, id)
        }

        fun delete(id: Any) {
            val idAsList = id.toList()
            onDelete(tableName, idAsList)
            innerDelete(idName, idAsList)
        }

        fun truncate() = clear()

        fun innerUpdate(fieldName2Find: String, id: Any, fieldName2Set: String, value: Any) {
            val field2Find = readPersisterData.field(fieldName2Find)
            val field2Set = readPersisterData.field(fieldName2Set)
            write("UPDATE $tableName SET ${field2Set.fNEqualsValue(value, comma)} ${field2Find.whereClause(id, comma)}")
            tableEvent()
        }

        /**
         * [fieldName2Set] is the fieldName of the field, that has to be reset by [value].
         * All rows are replaced by [value], where [fieldName2Find] = [id].
         *
         * e.g. UPDATE [clazz] SET [fieldName2Set] = [value] WHERE [fieldName2Find] = [id];
         */
        fun update(fieldName2Find: String, id: Any, fieldName2Set: String, value: Any) {
            onUpdate(tableName)
            innerUpdate(fieldName2Find, id, fieldName2Set, value)
        }

        /**
         * [fieldName2Set] is the fieldName of the field, that has to be reset by [value].
         * All rows are replaced by [value], where the primary key is [id].
         *
         * e.g. UPDATE [clazz] SET [fieldName2Set] = [value] WHERE [idName] = [id];
         */
        fun update(id: Any, fieldName2Set: String, value: Any) {
            onUpdate(tableName, id.toList())
            update(idName, id, fieldName2Set, value)
        }

        private fun <T : Any> readColumn(fieldName: String, cmd: (Any) -> String): List<T> {
            val clazz: KClass<T> = this.readPersisterData.classOfField(fieldName) as KClass<T>
            return if (clazz.java.isPrimitiveOrWrapperOrString()) {
                this@Persister.read(cmd(fieldName)) {
                    it.getList { clazz.cast(getObject(1)) }
                }
            } else {
                val readPersisterData = ReadPersisterData<T, Any>(clazz, this@Persister, readPersisterData.persisterProvider)
                val key = readPersisterData.createTableKeyData()
//                val key = readPersisterData.createTableKeyData(fieldName)
                this@Persister.read(cmd(key)) { it.getList { readPersisterData.readKey(ReadValue(this)) } as List<T> }
            }
        }

        /**
         * returns all distinct values of the column [fieldName]
         *
         * e.g. SELECT DISTINCT [fieldName] from [clazz];
         *
         * @since 0.0.8 -> group by is used instead of distinct
         */
        fun <T : Any> distinctValues(fieldName: String): List<T> {
            return readColumn(fieldName) { it -> "SELECT $it from $tableName GROUP BY $it;" }
        }

        private fun internReadAll(orderOrder: String = ""): List<R> {
            return this@Persister.read("SELECT * from $tableName $orderOrder;") {
                it.getList { readPersisterData.evaluate(ReadValue(this), readCache()) }
            }
        }


        /**
         * returns all data from the current Table [clazz], ordered by [ReadPersisterData.keyName]
         */
        fun readAll() = internReadAll("order by ${readPersisterData.keyName()}")

        /**
         * same as [readAll], but there is no guarantee about the order
         */
        fun readAllUnordered() = internReadAll()

        /**
         * returns all keys from the current Table [clazz]
         */
        fun <T : Any> readAllKeys(): List<T> {
            return readColumn(readPersisterData.keyColumnName()) { "SELECT $selectKeyHeader from $tableName;" }
        }

        private fun <X : Any> innerReadPlain(readHeader: String = "*", func: (ResultSet) -> X): X {
            return persister.read("Select $readHeader from $tableName;", func)
        }

        internal fun <X : Any> readPlain(list: List<SimpleReadObject>, listener: (List<Any>) -> X): List<X> {
            return innerReadPlain(list.joinToString(", ") { it.name }, readPlainMapper(list, listener))
        }

        fun <X : Any> requestBuilder(list: List<String>, listener: (List<Any>) -> X): RequestBuilder<X> {
            val res = list.map { name ->
                readPersisterData.fields.asSequence().map { it.plainType(name) }.dropWhile { it == null }.takeWhile { it != null }
                        .lastOrNull() ?: throw RuntimeException("there was no field with name $name found")
            }
            return RequestBuilder(res.toMutableList(), this, listener)
        }

        fun <X : Any> requestBuilder(string: String, listener: (List<Any>) -> X): RequestBuilder<X> {
            val list = string.split(",")
                    .map { it.trim() }
            return requestBuilder(list, listener)
        }

//        fun tableData(): TableData {
//            val values = this@Persister.read("SELECT * from $tableName;") { it.getList { readPersisterData.readToString { getObject(it) } } }
//            return TableData(_tableName, databaseInterface.path, readPersisterData.header(), values)
//        }

//        fun helperTables() = readPersisterData.helperTables()
//        fun keyTables() = readPersisterData.keyTables()

//        fun allTables() = AllTables(tableData(), helperTables(), keyTables())

//        fun readAllTableData(tables: AllTables): List<R> {
//            return tables.tableData.values.map { readPersisterData.evaluate(TableDataReadValue(tables, it)) }
//        }

        private fun timespread(whereClause: String): List<R> {
            return this@Persister.read("select * from $tableName where ${readPersisterData.keyName()} $whereClause") {
                it.getList { readPersisterData.evaluate(ReadValue(this), readCache()) }
            }
        }

        fun <K : Comparable<K>> read(from: K, to: K): List<R> {
            return timespread("between $from and $to;")
        }

        fun <K : Comparable<K>> read(from: K, to: K, max: Int): List<R> {
            return timespread("between $from and $to LIMIT $max;")
        }

        /**
         * [last] are the number of values that are trying to be read. [before] is the value, until it's read
         */
        fun <K : Comparable<K>> readLastBefore(last: Long, before: K): List<R> {
            val keyColumnName = readPersisterData.keyColumnName()
            return this@Persister.read("SELECT * FROM ( select * from $tableName where $keyColumnName < $before ORDER BY $keyColumnName DESC LIMIT $last ) X ORDER BY $keyColumnName ASC") {
                it.getList { readPersisterData.evaluate(ReadValue(this), readCache()) }
            }
        }

        /**
         * [maxNumber] are the number of values that are trying to be read. [after] is the value, after it's read
         */
        fun <K : Comparable<K>> readNextAfter(maxNumber: Long, after: K): List<R> {
            return timespread("> $after LIMIT $maxNumber;")
        }

        fun last(): R? {
            val keyColumnName = readPersisterData.keyColumnName()
            val req = "SELECT * FROM $tableName WHERE $keyColumnName = (SELECT max($keyColumnName) from $tableName);"
            val x = this@Persister.read(req) { it.getList { readPersisterData.evaluate(ReadValue(this), readCache()) } }
            return x.firstOrNull()
        }

        fun first(): R? {
            val req = "SELECT * FROM $tableName LIMIT 1;"
            val x = this@Persister.read(req) { it.getList { readPersisterData.evaluate(ReadValue(this), readCache()) } }
            return x.firstOrNull()
        }

        fun size(): Int {
            val keyColumnName = readPersisterData.keyColumnName()
            val req = "SELECT COUNT($keyColumnName) FROM $tableName;"

            val x = this@Persister.read(req) { it.getInt(1) }
            return x
        }

//        fun resetTable(list: List<R>) {
//            list.forEach {
//                val id = readPersisterData.keySimpleType(it)
//                if (exists(id)) {
//                    delete(id)
//                }
//                insert(it)
//            }
//        }
    }

    companion object {
        private const val comma = ", "
        private const val and = " and "
        internal const val createTable = "CREATE TABLE IF NOT EXISTS"
    }
}
