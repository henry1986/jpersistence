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

import mu.KotlinLogging
import org.daiv.reflection.annotations.MoreKeys
import org.daiv.reflection.common.*
import org.daiv.reflection.database.DatabaseInterface
import org.daiv.reflection.isPrimitiveOrWrapperOrString
import org.daiv.reflection.read.*
import org.daiv.util.DefaultRegisterer
import org.daiv.util.Registerer
import java.sql.ResultSet
import java.sql.SQLException
import kotlin.reflect.KClass
import kotlin.reflect.full.cast
import kotlin.reflect.full.findAnnotation

/**
 * @author Martin Heinrich
 */
class Persister(private val databaseInterface: DatabaseInterface,
                private val registerer: DefaultRegisterer<DBChangeListener> = DefaultRegisterer()) :
        Registerer<DBChangeListener> by registerer {

    private fun event() {
        registerer.forEach(DBChangeListener::onChange)
    }

    internal fun <T : Any> read(query: String, func: (ResultSet) -> T): T {
        try {
            logger.trace(query)
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
            logger.trace(query)
            val statement = databaseInterface.statement
            statement.execute(query)
            statement.close()
        } catch (e: SQLException) {
            throw RuntimeException("query: $query", e)
        }
    }

    private fun getTableName(tableName: String, clazz: KClass<*>) = if (tableName == "") clazz.tableName() else tableName

    internal interface InternalTable<R : Any> {
        val readPersisterData: InternalRPD<out Any, Any>
        val tableName: String
        val _tableName: String
        val persister: Persister

        fun dropTable() {
            persister.write("DROP TABLE $tableName;")
        }

        fun <T : InternalTable<R>> rename(newTableName: String, creator: () -> T): T {
            persister.write("ALTER TABLE $tableName RENAME TO `$newTableName`")
            return creator()
        }

        fun namesToMap() = readPersisterData.names()

        fun copyData(newVariables: Map<String, String>, tableName: String) {
            val next = this
            val command = next.readPersisterData.copyTable(next.namesToMap() + newVariables)
            persister.write("INSERT INTO ${next.tableName} $command from $tableName;")
        }

        fun <T : Any, S : InternalTable<T>> change(next: InternalTable<T>,
                                                   newVariables: Map<String, String> = mapOf(),
                                                   creator: (String) -> S): S {
            next.persist()
            next.copyData(newVariables, tableName)
            dropTable()
            return next.rename(_tableName) { creator(_tableName) }
        }

        fun persist() {
            persister.write("$createTable $tableName ${readPersisterData.createTable()}")
            readPersisterData.createTableForeign()
            persister.event()
        }

        fun fromWhere(fieldName: String, id: Any, sep: String): String {
            try {
                val x = " FROM $tableName ${readPersisterData.fromWhere(fieldName, id, sep)}"
                return x
            } catch (t: Throwable) {
                throw t
            }
        }

        fun readIntern(fnEqualsValue: String, orderOrder: String = ""): List<List<ReadFieldValue>> {
//            val req = "SELECT $selectHeader FROM $tableName $join ${whereClause(fieldName, id, and)};"
            val req = "SELECT * FROM $tableName WHERE $fnEqualsValue $orderOrder;"
            return persister.read(req) { it.getList { readPersisterData.readWOObject(ReadValue(this)) } }
        }

        fun insertListBy(insertObjects: List<List<InsertObject>>) {
            if (insertObjects.isEmpty()) {
                return
            }
            persister.write("INSERT INTO $tableName ${readPersisterData.insertListBy(insertObjects)}")
//            readPersisterData.insertListBy(insertObjects)
            persister.event()
        }

        fun clear() {
            persister.write("DELETE from $tableName;")
            readPersisterData.clearLists()
            persister.event()
        }

        fun deleteBy(fieldName: String, id: Any) {
            try {
                persister.write("DELETE ${fromWhere(fieldName, id, comma)};")
                persister.event()
            } catch (e: Throwable) {
                throw e
            }
        }

    }

    internal inner class HelperTable(val fields: List<FieldData<Any, Any, Any, Any>>, tableName: String, numberOfKeyFields: Int = 1) :
            InternalTable<Any> {
        override val readPersisterData: InternalRPD<out Any, Any> = object : InternalRPD<Any, Any> {
            override val fields: List<FieldData<Any, Any, Any, Any>> = this@HelperTable.fields
            override val key: KeyType = KeyType(fields.take(numberOfKeyFields))
        }
        override val tableName: String = tableName
        override val _tableName: String = tableName
        override val persister: Persister = this@Persister

    }

    inner class Table<R : Any> internal constructor(override val readPersisterData: ReadPersisterData<R, Any>,
                                                    override val _tableName: String) :
            InternalTable<R>, Registerer<DBChangeListener> by registerer {
        override val tableName = "`$_tableName`"
        override val persister = this@Persister

        constructor(clazz: KClass<R>, tableName: String = "")

                : this(ReadPersisterData(clazz,
                                         null,
                                         this@Persister,
                                         getTableName(tableName, clazz)),
                       getTableName(tableName, clazz))

        //        private val readPersisterData: ReadPersisterData<R> = ReadPersisterData.create(clazz, this@Persister)
        private val registerer: DefaultRegisterer<DBChangeListener> = DefaultRegisterer()

        private val selectHeader by lazy { readPersisterData.underscoreName() }
        private val selectKeyHeader by lazy { readPersisterData.keyName() }
        //        private val join by lazy {
//            readPersisterData.joinNames(tableName)
//                .map { "INNER JOIN ${it.join()}" }
//                .joinToString(" ")
//        }
        private val idName = readPersisterData.getIdName()

        private fun tableEvent() {
            registerer.forEach(DBChangeListener::onChange)
            event()
        }

        fun rename(newTableName: String): Table<R> {
            return this.rename(newTableName) { Table(readPersisterData, newTableName) }
        }

        fun <T : Any> change(clazz: KClass<T>, newVariables: Map<String, String> = mapOf()): Table<T> {
            val tName = "${_tableName}_temp"
            val next = persister.Table(clazz, tName)
            return change(next, newVariables) { Table(next.readPersisterData, it) }
        }

        fun copyHelperTable(map: Map<String, Map<String, String>>): Table<R> {
            readPersisterData.copyHelperTable(map)
            return this
        }

        fun read(fieldName: String, id: Any, orderOrder: String = ""): List<R> {
//            val req = "SELECT $selectHeader FROM $tableName $join ${whereClause(fieldName, id, and)};"
            val req = "SELECT * ${fromWhere(fieldName, id, and)} $orderOrder;"
            return this@Persister.read(req) { it.getList { readPersisterData.evaluate(ReadValue(this)) } }
        }

//        fun readMultiple(fieldName: List<String>, id: List<Any>, orderOrder: String = ""): List<R> {
////            val req = "SELECT $selectHeader FROM $tableName $join ${whereClause(fieldName, id, and)};"
//            val req = "SELECT * ${fromWhere(fieldName, id, and)} $orderOrder;"
//            return this@Persister.read(req) { it.getList { readPersisterData.evaluate(ReadValue(this)) } }
//        }

        fun readOrdered(fieldName: String, id: Any): List<R> {
            return read(fieldName, id, "ORDER BY ${readPersisterData.keyName()}")
        }

        private fun Any.toList(): List<*> {
            val req = if (this is List<*>) {
                this
            } else
                listOf(this)
            return req
        }

        fun read(id: Any): R? {
            return read(idName, id.toList()).firstOrNull()
        }

        fun readMultiple(id: List<Any>): R? {
            return read(idName, id).firstOrNull()
        }

//        fun readMultiple(vararg id: Any): R? {
//            return readMultiple(id.asList())
//        }

//        fun read(id: List<Any>): R? {
//            return read(idName, id).firstOrNull()
//        }

        fun insert(o: R) {
            val createTable = "INSERT INTO $tableName ${readPersisterData.insert(o)}"
            write(createTable)
            readPersisterData.insertInnerFieldCollections(o)
            tableEvent()
        }

        fun insert(o: List<R>) {
            if (o.isEmpty()) {
                return
            }
            write("INSERT INTO $tableName ${readPersisterData.insertList(o)}")
            readPersisterData.insertLists(o)
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

        fun delete(fieldName: String, id: Any) {
            try {
                val list = read(fieldName, id)
                this@Persister.write("DELETE ${fromWhere(fieldName, id, comma)};")
                list.forEach { readPersisterData.deleteLists(it) }
                tableEvent()
            } catch (e: Throwable) {
                throw e
            }
        }

        fun delete(id: Any) {
            delete(idName, id.toList())
        }

        fun truncate() = clear()

        /**
         * [fieldName2Set] is the fieldName of the field, that has to be reset by [value].
         * All rows are replaced by [value], where [fieldName2Find] = [id].
         *
         * e.g. UPDATE [clazz] SET [fieldName2Set] = [value] WHERE [fieldName2Find] = [id];
         */
        fun update(fieldName2Find: String, id: Any, fieldName2Set: String, value: Any) {
            val field2Find = readPersisterData.field(fieldName2Find)
            val field2Set = readPersisterData.field(fieldName2Set)
            write("UPDATE $tableName SET ${field2Set.fNEqualsValue(value, comma)} ${field2Find.whereClause(id, comma)}")
            tableEvent()
        }

        /**
         * [fieldName2Set] is the fieldName of the field, that has to be reset by [value].
         * All rows are replaced by [value], where the primary key is [id].
         *
         * e.g. UPDATE [clazz] SET [fieldName2Set] = [value] WHERE [idName] = [id];
         */
        fun update(id: Any, fieldName2Set: String, value: Any) {
            update(idName, id, fieldName2Set, value)
        }

        private fun <T : Any> readColumn(fieldName: String, cmd: (Any) -> String): List<T> {
            val clazz: KClass<T> = this.readPersisterData.classOfField(fieldName) as KClass<T>
            return if (clazz.java.isPrimitiveOrWrapperOrString()) {
                this@Persister.read(cmd(fieldName)) {
                    it.getList { clazz.cast(getObject(1)) }
                }
            } else {
                val readPersisterData = ReadPersisterData<T, Any>(clazz, null, this@Persister)
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
                it.getList { readPersisterData.evaluate(ReadValue(this)) }
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
            return readColumn(idName) { "SELECT $selectKeyHeader from $tableName;" }
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
                it.getList { readPersisterData.evaluate(ReadValue(this)) }
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
                it.getList { readPersisterData.evaluate(ReadValue(this)) }
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
            val x = this@Persister.read(req) { it.getList { readPersisterData.evaluate(ReadValue(this)) } }
            return x.firstOrNull()
        }

        fun first(): R? {
            val req = "SELECT * FROM $tableName LIMIT 1;"
            val x = this@Persister.read(req) { it.getList { readPersisterData.evaluate(ReadValue(this)) } }
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
        private val logger = KotlinLogging.logger {}
    }
}
