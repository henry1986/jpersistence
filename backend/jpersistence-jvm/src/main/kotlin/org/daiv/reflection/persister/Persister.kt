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
import org.daiv.reflection.annotations.AllTables
import org.daiv.reflection.annotations.TableData
import org.daiv.reflection.common.*
import org.daiv.reflection.database.DatabaseInterface
import org.daiv.reflection.isPrimitiveOrWrapperOrString
import org.daiv.reflection.read.ReadPersisterData
import org.daiv.util.DefaultRegisterer
import org.daiv.util.Registerer
import org.sqlite.core.DB
import java.sql.ResultSet
import java.sql.SQLException
import kotlin.reflect.KClass
import kotlin.reflect.full.cast

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

    inner class Table<R : Any> internal constructor(private val readPersisterData: ReadPersisterData<R, Any>,
                                                    val _tableName: String) :
            Registerer<DBChangeListener> by registerer {
        private val tableName = "`$_tableName`"

        constructor(clazz: KClass<R>, tableName: String = "")
                : this(ReadPersisterData(clazz, null, this@Persister, getTableName(tableName, clazz)), getTableName(tableName, clazz))

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

        fun persist() {
            write("$createTable $tableName ${readPersisterData.createTable()}")
            readPersisterData.createTableForeign()
            event()
        }

        /**
         * returns "[fieldName] = [id]" for primitive Types, or the complex variant
         * for complex types, the [sep] is needed
         */
        private fun fNEqualsValue(field: FieldData<R, Any, Any, Any>, id: Any, sep: String): String {
            return field.fNEqualsValue(id, sep)
//            return when {
//                id::class.java.isPrimitiveOrWrapperOrString() -> {
//                    "$fieldName = ${ReadSimpleType.makeString(id)}"
//                }
//                id::class.isSubclassOf(Enum::class) -> {
//                    "$fieldName = ${EnumType.makeString(id)}"
//                }
//                else -> {
//                    val persisterData = ReadPersisterData.create<Any, Any>(id::class as KClass<Any>, this@Persister)
//                    "${persisterData.fNEqualsValue(id, fieldName, sep)}"
//                }
//            }
        }

        private fun whereClause(field: FieldData<R, Any, Any, Any>, id: Any, sep: String): String {
            return "WHERE ${fNEqualsValue(field, id, sep)}"
        }

        private fun fromWhere(fieldName: String, id: Any, sep: String): String {
            return " FROM $tableName ${whereClause(readPersisterData.field(fieldName) as FieldData<R, Any, Any, Any>, id, sep)}"
        }


        fun read(fieldName: String, id: Any): List<R> {
//            val req = "SELECT $selectHeader FROM $tableName $join ${whereClause(fieldName, id, and)};"
            val req = "SELECT * ${fromWhere(fieldName, id, and)};"
            return this@Persister.read(req) { it.getList { readPersisterData.evaluate(DBReadValue(this)) } }
        }


        fun read(id: Any): R? {
            return read(idName, id).firstOrNull()
        }

        fun insert(o: R) {
            val createTable = "INSERT INTO $tableName ${readPersisterData.insert(o)}"
            write(createTable)
            readPersisterData.insertLists(o)
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
            return exists(idName, id)
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
            delete(idName, id)
        }

        fun clear() {
            this@Persister.write("DELETE from $tableName;")
            readPersisterData.clearLists()
            tableEvent()
        }

        /**
         * [fieldName2Set] is the fieldName of the field, that has to be reset by [value].
         * All rows are replaced by [value], where [fieldName2Find] = [id].
         *
         * e.g. UPDATE [clazz] SET [fieldName2Set] = [value] WHERE [fieldName2Find] = [id];
         */
        fun update(fieldName2Find: String, id: Any, fieldName2Set: String, value: Any) {
            val field2Find = readPersisterData.field(fieldName2Find) as FieldData<R, Any, Any, Any>
            val field2Set = readPersisterData.field(fieldName2Set) as FieldData<R, Any, Any, Any>
            write("UPDATE $tableName SET ${fNEqualsValue(field2Set, value, comma)} ${whereClause(field2Find, id, comma)}")
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
                this@Persister.read(cmd(key)) { it.getList { readPersisterData.readKey(DBReadValue(this)) } as List<T> }
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

        /**
         * returns all data from the current Table [clazz]
         */
        fun readAll(): List<R> {
            return this@Persister.read("SELECT * from $tableName;") {
                it.getList { readPersisterData.evaluate(DBReadValue(this)) }
            }
        }

        /**
         * returns all keys from the current Table [clazz]
         */
        fun <T : Any> readAllKeys(): List<T> {
            return readColumn(idName) { "SELECT $selectKeyHeader from $tableName;" }
        }

        fun tableData(): TableData {
            val values = this@Persister.read("SELECT * from $tableName;") { it.getList { readPersisterData.readToString { getObject(it) } } }
            return TableData(_tableName, databaseInterface.path, readPersisterData.header(), values)
        }

        fun helperTables() = readPersisterData.helperTables()
        fun keyTables() = readPersisterData.keyTables()

        fun allTables() = AllTables(tableData(), helperTables(), keyTables())

        fun readAllTableData(tables: AllTables): List<R> {
            return tables.tableData.values.map { readPersisterData.evaluate(TableDataReadValue(tables, it)) }
        }

        private fun timespread(whereClause: String): List<R> {
            val keyColumnName = readPersisterData.onKey { prefixedName }
            return this@Persister.read("select * from $tableName where $keyColumnName $whereClause") {
                it.getList { readPersisterData.evaluate(DBReadValue(this)) }
            }
        }

        fun <K : Comparable<K>> read(from: K, to: K): List<R> {
            return timespread("between $from and $to;")
        }

        /**
         * [last] are the number of values that are trying to be read. [before] is the value, until it's read
         */
        fun <K : Comparable<K>> readLastBefore(last: Long, before: K): List<R> {
            val keyColumnName = readPersisterData.onKey { prefixedName }
            return this@Persister.read("SELECT * FROM ( select * from $tableName where $keyColumnName < $before ORDER BY $keyColumnName DESC LIMIT $last ) X ORDER BY $keyColumnName ASC") {
                it.getList { readPersisterData.evaluate(DBReadValue(this)) }
            }
        }

        /**
         * [maxNumber] are the number of values that are trying to be read. [after] is the value, after it's read
         */
        fun <K : Comparable<K>> readNextAfter(maxNumber: Long, after: K): List<R> {
            return timespread("> $after LIMIT $maxNumber;")
        }

        fun readTableData(tables: AllTables, id: Any): R {
            return readPersisterData.evaluate(TableDataReadValue(tables,
                                                                 tables.tableData.values.find { it.first() == id.toString() }!!))
        }

        fun readTableData(tables: AllTables, tableData: TableData, fieldName: String, id: Any): List<R> {
            val i = tableData.header.indexOf(fieldName)
            return tableData.values.filter { it[i] == id.toString() }
                    .map { readPersisterData.evaluate(TableDataReadValue(tables, it)) }
        }

        fun last(): R? {
            val keyColumnName = readPersisterData.onKey { prefixedName }
            val req = "SELECT * FROM $tableName WHERE $keyColumnName = (SELECT max($keyColumnName) from $tableName);"
            val x = this@Persister.read(req) { it.getList { readPersisterData.evaluate(DBReadValue(this)) } }
            return x.firstOrNull()
        }

        fun first(): R? {
            val req = "SELECT * FROM $tableName LIMIT 1;"
            val x = this@Persister.read(req) { it.getList { readPersisterData.evaluate(DBReadValue(this)) } }
            return x.firstOrNull()
        }

        fun size(): Int {
            val keyColumnName = readPersisterData.onKey { prefixedName }
            val req = "SELECT COUNT($keyColumnName) FROM $tableName;"

            val x = this@Persister.read(req) { it.getInt(1) }
            return x
        }

        fun resetTable(list: List<R>) {
            list.forEach {
                val id = readPersisterData.keySimpleType(it)
                if (exists(id)) {
                    delete(id)
                }
                insert(it)
            }
        }

        fun dropTable() {
            this@Persister.write("DROP TABLE $tableName;")
        }
    }

    companion object {
        private const val comma = ", "
        private const val and = " and "
        internal const val createTable = "CREATE TABLE IF NOT EXISTS"
        private val logger = KotlinLogging.logger {}
    }
}
