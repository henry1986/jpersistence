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
import org.daiv.reflection.plain.ObjectKey
import org.daiv.reflection.plain.RequestBuilder
import org.daiv.reflection.plain.SimpleReadObject
import org.daiv.reflection.plain.readPlainMapper
import org.daiv.reflection.read.*
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
class Persister constructor(private val databaseInterface: DatabaseInterface,
                            _mapper: List<Mapper<*, *>> = emptyList(),
                            val persisterPreference: PersisterPreference = defaultPersisterPreference(),
                            private val registerer: DefaultRegisterer<DBChangeListener> = DefaultRegisterer()) :
        Registerer<DBChangeListener> by registerer {
    constructor(dbPath: String, persisterPreference: PersisterPreference = defaultPersisterPreference()) :
            this(DatabaseHandler(dbPath), emptyList(), persisterPreference)

    constructor(dbPath: String, mapper: List<Mapper<*, *>>, persisterPreference: PersisterPreference = defaultPersisterPreference()) :
            this(DatabaseHandler(dbPath), mapper, persisterPreference)

    fun close() = databaseInterface.close()
    internal val mapper = (_mapper.map { it.origin to it })
            .toMap()
    internal val backmapper = (_mapper.map { it.mapped to it })
            .toMap()
    internal val clazzMapper = _mapper.map { it.origin to it.mapped }.toMap()

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

    internal fun readCache() = internalReadCache

    fun clearCache() = readCache().clear()

    internal fun justPersist(tableName: String, readPersisterData: InternalRPD) {
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

    private fun innerCachePreference() = InsertCachePreference(false, true)
    private fun CoroutineScope.actors(requestScope: CoroutineScope,
                                      completable: Boolean,
                                      persisterProvider: PersisterProvider) = (persisterProvider.allTables().map {
        val actorHandler = ActorHandler(this, requestScope, readCache().readTableCache(it.value, persisterProvider.isAutoIdTable(it.key)))
        it.value to if (!completable) InsertActor(actorHandler) else InsertCompletableActor(actorHandler)
    } + persisterProvider.getHelperTableNames().map {
        val actorHandler = ActorHandler(this, requestScope, readCache().readTableCache(it, false))
        it to if (!completable) InsertActor(actorHandler) else InsertCompletableActor(actorHandler)
    })
            .toMap()


    interface CommonCache {
        fun <R : Any> onTable(table: Table<R>): InsertCache<R>
        fun showCacheList(table: Table<*>): Map<List<Any>, Any>
        fun showCacheDoubleList(table: Table<*>): Map<List<Any>, List<Any>>
        fun showDoublesOnly(table: Table<*>) = showCacheDoubleList(table).filter { it.value.size > 1 }
        fun insertPartial(table: Table<*>, block: (Int, Any) -> Boolean): InsertPartialAnswer
        fun commit()
    }

    internal interface InternalCommonCache : CommonCache {
        val persister: Persister
        val actors: Map<InternalTable, TableHandler>
        val i: InsertMap

        override fun showCacheList(table: Table<*>): Map<List<Any>, Any> {
            val actor = actors[table]!!
            return actor.debugList()
        }

        override fun showCacheDoubleList(table: Table<*>): Map<List<Any>, List<Any>> {
            val actor = actors[table]!!
            return actor.doubleList()
        }


        override fun insertPartial(table: Table<*>, block: (Int, Any) -> Boolean): InsertPartialAnswer {
            return i.insertPartial(table, block)
        }

        override fun commit() {
            i.insertAll()
            persister.event()
        }
    }

    inner class ParallelCommonCache private constructor(private val internalParallelCommonCache: InternalParallelCommonCache) :
            CommonCache by internalParallelCommonCache {
        constructor(coroutineScope: CoroutineScope,
                    requestScope: CoroutineScope,
                    completable: Boolean,
                    insertCachePreference: InsertCachePreference = innerCachePreference(),
                    tableNamePrefix: String? = null) :
                this(InternalParallelCommonCache(coroutineScope, requestScope, completable, insertCachePreference, tableNamePrefix))
    }

    inner class SeriellCommonCache private constructor(private val internalSerialCommonCache: InternalSerialCommonCache) :
            CommonCache by internalSerialCommonCache {
        constructor(insertCachePreference: InsertCachePreference = innerCachePreference(),
                    tableNamePrefix: String? = null) : this(InternalSerialCommonCache(insertCachePreference, tableNamePrefix))
    }

    internal inner class InternalParallelCommonCache(coroutineScope: CoroutineScope,
                                                     requestScope: CoroutineScope,
                                                     completable: Boolean,
                                                     insertCachePreference: InsertCachePreference = innerCachePreference(),
                                                     tableNamePrefix: String? = null) : InternalCommonCache {
        private val p = persisterProviderMap[tableNamePrefix]!!
        override val persister: Persister
            get() = this@Persister
        override val actors = coroutineScope.actors(requestScope, completable, p)
        override val i = InsertMap(this@Persister, insertCachePreference, actors, readCache())

        override fun <R : Any> onTable(table: Table<R>): InsertCache<R> {
            return table.insertCache(this, true)
        }
    }

    internal inner class InternalSerialCommonCache(insertCachePreference: InsertCachePreference = innerCachePreference(),
                                                   tableNamePrefix: String? = null) : InternalCommonCache {
        override val persister: Persister
            get() = this@Persister
        private val p = persisterProviderMap[tableNamePrefix]!!
        override val actors = readCache().tableHandlers(p)
        override val i = InsertMap(this@Persister, insertCachePreference, actors, readCache())

        override fun <R : Any> onTable(table: Table<R>): InsertCache<R> {
            return table.insertCache(this, false)
        }
    }

    private fun getTableName(tableName: String, clazz: KClass<*>) = if (tableName == "") clazz.tableName() else tableName


    internal interface InternalTable : PersisterListener {
        val readPersisterData: InternalRPD
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

        fun fromWhere(fieldName: String, id: Any, sep: String): String? {
            try {
                return readPersisterData.fromWhere(fieldName, id, sep, persister.readCache())
                        ?.let {
                            val x = " FROM $tableName $it"
                            x
                        }
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

    internal inner class HelperTable constructor(val keyFields: List<FieldData>,
                                                 val fields: List<FieldData>,
                                                 val noKeyFields: List<FieldData>,
                                                 val innerTableNameGetter: () -> String,
                                                 val tableNameGetter: () -> String) :
            InternalTable, PersisterListener by internalReadCache {

        override val readPersisterData: InternalRPD = object : InternalRPD {
            override val fields: List<FieldData> = this@HelperTable.fields
            override val key: KeyType = KeyType(keyFields)
            override val noKeyFields: List<FieldData> = this@HelperTable.noKeyFields
        }

        override val innerTableName: String
            get() = innerTableNameGetter()
        override val tableName: String
            get() = tableNameGetter()
        override val _tableName: String
            get() = tableName
        override val persister: Persister = this@Persister

        override fun toString(): String {
            return innerTableName
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as HelperTable

            if (tableName != other.tableName) return false

            return true
        }

        override fun hashCode(): Int {
            return tableName.hashCode()
        }


    }

    private fun <R : Any> createPersisterProvider(clazz: KClass<R>,
                                                  tableName: String,
                                                  tableNamePrefix: String?): PersisterProvider {
        val persisterProvider = PersisterProviderImpl(this@Persister, tableNamePrefix)
        persisterProvider[clazz] = getTableName(tableName, clazz)
        return persisterProvider
    }

    private fun createPersisterProvider(tableNamePrefix: String?) = PersisterProviderImpl(this@Persister, tableNamePrefix)

    private val persisterProviderMap = mutableMapOf<String?, PersisterProvider>(null to createPersisterProvider(null))

    internal fun persisterProviderForTest(tableNamePrefix: String?): PersisterProvider? {
        return persisterProviderMap[tableNamePrefix]
    }

    internal fun <R : Any> getPersisterProvider(clazz: KClass<R>,
                                                tableName: String,
                                                tableNamePrefix: String?): PersisterProvider {
        val p = persisterProviderMap[tableNamePrefix]
        return if (p == null) {
            val x = createPersisterProvider(tableNamePrefix)
            x[clazz] = getTableName(tableName, clazz)
            persisterProviderMap[tableNamePrefix] = x
            x
        } else {
            p.setIfNotExists(clazz, getTableName(tableName, clazz))
            p
        }
    }

    fun <R : Any> tableOnOwnPersisterProvider(clazz: KClass<R>,
                                              tableName: String = "",
                                              tableNamePrefix: String? = null): Table<R> {
        return Table(clazz, createPersisterProvider(clazz, tableName, tableNamePrefix))
    }

    inner class Table<R : Any> internal constructor(override val readPersisterData: ReadPersisterData,
                                                    val clazz: KClass<in R>) :
            InternalTable, Registerer<DBChangeListener> by registerer, PersisterListener by internalReadCache {
        override val innerTableName: String
            get() = readPersisterData.persisterProvider.innerTableName(clazz)
        override val _tableName
            get() = readPersisterData.persisterProvider[clazz]

        override fun toString(): String {
            return clazz.toString()
        }

        internal fun readCache() = this@Persister.readCache()
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
                    tableNamePrefix: String? = null)
                : this(clazz, getPersisterProvider(clazz, tableName, tableNamePrefix))

        internal constructor(clazz: KClass<R>, persisterProvider: PersisterProvider)
                : this(ReadPersisterData(clazz as KClass<Any>, this@Persister, persisterProvider), clazz)

        init {
            persisterProvider.setAutoTableId(clazz, readPersisterData.key.isAuto(), this)
        }

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
            persisterProvider.remove(clazz)
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
            oldTable?.let {
                persisterProvider[it.clazz] = _tableName
            }
            val request: (String, String) -> String = { helperTableName, variable ->
                "(select $variable from $tableName where ${oldTable?.toKeyNames(helperTableName)})"
            }
            readPersisterData.copyHelperTable(map, request)
            return this
        }

        internal fun read(fieldName: String, id: Any, readCache: ReadCache, orderOrder: String = ""): List<R> {
            val req = fromWhere(fieldName, id, and)?.let {
                "SELECT * $it $orderOrder;"
            } ?: return emptyList()
            return this@Persister.read(req) { it.getList { readPersisterData.evaluate(ReadValue(this), readCache) } } as List<R>
        }

        fun read(fieldName: String, id: Any, orderOrder: String = ""): List<R> {
            return read(fieldName, id, readCache(), orderOrder).map { it }
        }

        fun clearCache() {
            readCache().clear()
        }

        fun readOrdered(fieldName: String, id: Any): List<R> {
            return read(fieldName,
                        id,
                        readCache(),
                        orderOrder = "ORDER BY ${readPersisterData.keyName()}").map { it }
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

        private fun List<Any>.toHashCodeX(): R? {
            if (readPersisterData.key.isAuto()) {
                val hashCodeX = readPersisterData.key.plainHashCodeX(this)
                val read = readHashCode(hashCodeX, readCache())
                return read.find { readPersisterData.getKey(it.second) == this }
                        ?.second
            }
            return read(idName, this, readCache()).firstOrNull()
        }

        fun read(id: Any): R? {
            val key = id.toList()
            return key.toHashCodeX()
//            return read(idName, id.toList().toHashCodeX(), readCache()).firstOrNull()
        }

        fun readMultiple(id: List<Any>): R? {
            return id.toHashCodeX()
        }

        internal fun innerReadMultipleUseHashCode(id: ObjectKey, readCache: ReadCache): R? {
            val read = read(idName, id.keys(), readCache)
            return read.firstOrNull()
        }

        internal fun readHashCode(hashCode: Int, readCache: ReadCache): List<Pair<Int, R>> {
            val internRead = readIntern(readPersisterData.key.fields.first().fNEqualsValue(hashCode, and, readCache)!!, readCache)
            return internRead.map { it[1].value as Int to readPersisterData.method(it) } as List<Pair<Int, R>>
        }

        fun insert(o: R) {
            insert(listOf(o))
        }

        internal fun insertCache(insertCachePreference: InsertCachePreference,
                                 actors: Map<InternalTable, TableHandler>,
                                 isParallel: Boolean) =
                InsertCacheHandler(InsertMap(persister, insertCachePreference, actors, readCache()), readPersisterData, this, isParallel)

        inner class InsertCacheParallel(val coroutineScope: CoroutineScope, val requestScope: CoroutineScope, completable: Boolean,
                                        val insertCachePreference: InsertCachePreference = innerCachePreference()) :
                InsertCache<R> by insertCache(insertCachePreference,
                                              coroutineScope.actors(requestScope, completable, persisterProvider),
                                              true)

        inner class InsertCacheSeriell(val insertCachePreference: InsertCachePreference = innerCachePreference()) :
                InsertCache<R> by insertCache(insertCachePreference, readCache().tableHandlers(persisterProvider), false)

        internal fun insertCache(commonCache: InternalCommonCache, isParallel: Boolean): InsertCache<R> {
            return InsertCacheHandler(commonCache.i, readPersisterData, this, isParallel)
        }

        fun insert(o: List<R>, innerCachePreference: InsertCachePreference = innerCachePreference()) {
            if (o.isEmpty()) {
                return
            }
            runBlocking {
                val map = InsertMap(persister, innerCachePreference, readCache().tableHandlers(persisterProvider), readCache())
                readPersisterData.putInsertRequests(map, o, this@Table)
                map.insertAll()
            }
            tableEvent()
        }

        fun exists(fieldName: String, id: Any): Boolean {
            return this@Persister.read("SELECT EXISTS( SELECT $selectHeader ${fromWhere(fieldName,
                                                                                        id,
                                                                                        comma)});") { it.getInt(1) != 0 }
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
            write("UPDATE $tableName SET ${field2Set.fNEqualsValue(value, comma, readCache())} ${field2Find.whereClause(id,
                                                                                                                        comma,
                                                                                                                        readCache())}")
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
                val readPersisterData = ReadPersisterData(clazz as KClass<Any>, this@Persister, readPersisterData.persisterProvider)
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
                it.getList { readPersisterData.evaluate(ReadValue(this), readCache()) } as List<R>
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
            return this@Persister.read("select * from $tableName where ${readPersisterData.firstColumnName()} $whereClause") {
                it.getList { readPersisterData.evaluate(ReadValue(this), readCache()) } as List<R>
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
                it.getList { readPersisterData.evaluate(ReadValue(this), readCache()) } as List<R>
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
            return x.firstOrNull() as R?
        }

        fun first(): R? {
            val req = "SELECT * FROM $tableName LIMIT 1;"
            val x = this@Persister.read(req) { it.getList { readPersisterData.evaluate(ReadValue(this), readCache()) } }
            return x.firstOrNull() as R?
        }

        fun size(): Int {
            val keyColumnName = readPersisterData.keyColumnName()
            val req = "SELECT COUNT($keyColumnName) FROM $tableName;"

            val x = this@Persister.read(req) { it.getInt(1) }
            return x
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as Table<*>

            if (_tableName != other._tableName) return false

            return true
        }

        override fun hashCode(): Int {
            return _tableName.hashCode()
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
