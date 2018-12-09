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
import org.daiv.reflection.common.DBReadValue
import org.daiv.reflection.common.TableDataReadValue
import org.daiv.reflection.common.getList
import org.daiv.reflection.common.tableName
import org.daiv.reflection.database.DatabaseInterface
import org.daiv.reflection.isPrimitiveOrWrapperOrString
import org.daiv.reflection.read.ReadPersisterData
import org.daiv.reflection.read.ReadSimpleType
import org.daiv.util.DefaultRegisterer
import org.daiv.util.Registerer
import java.sql.ResultSet
import java.sql.SQLException
import kotlin.reflect.KClass
import kotlin.reflect.full.cast

/**
 * @author Martin Heinrich
 */
class Persister(val databaseInterface: DatabaseInterface,
                private val registerer: DefaultRegisterer<DBChangeListener> = DefaultRegisterer()) :
    Registerer<DBChangeListener> by registerer {

    private fun event() {
        registerer.forEach(DBChangeListener::onChange)
    }

    internal fun read(query: String): ResultSet {
        try {
            logger.debug(query)
            return databaseInterface.statement.executeQuery(query)
        } catch (e: SQLException) {
            throw RuntimeException("query: $query", e)
        }
    }

    internal fun write(query: String) {
        try {
            logger.debug(query)
            databaseInterface.statement.execute(query)
        } catch (e: SQLException) {
            throw RuntimeException("query: $query", e)
        }
    }

    inner class Table<R : Any> internal constructor(private val readPersisterData: ReadPersisterData<R, Any>,
                                                    val _tableName: String) :
        Registerer<DBChangeListener> by registerer {
        private val tableName = "`$_tableName`"

        constructor(clazz: KClass<R>, tableName: String = "")
                : this(ReadPersisterData.create(clazz, this@Persister),
                       if (tableName == "") clazz.tableName() else tableName)

        //        private val readPersisterData: ReadPersisterData<R> = ReadPersisterData.create(clazz, this@Persister)
        private val registerer: DefaultRegisterer<DBChangeListener> = DefaultRegisterer()

        private val selectHeader by lazy { readPersisterData.underscoreName() }
        private val selectKeyHeader by lazy { readPersisterData.keyName(null) }
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
        private fun fNEqualsValue(fieldName: String, id: Any, sep: String): String {
            return if (id::class.java.isPrimitiveOrWrapperOrString()) {
                "$fieldName = ${ReadSimpleType.makeString(id)}"
            } else {
                val writePersisterData = ReadPersisterData.create<Any, Any>(id::class as KClass<Any>, this@Persister)
                "${writePersisterData.fNEqualsValue(id, fieldName, sep)}"
            }
        }

        private fun whereClause(fieldName: String, id: Any, sep: String): String {
            return "WHERE ${fNEqualsValue(fieldName, id, sep)}"
        }

        private fun fromWhere(fieldName: String, id: Any, sep: String): String {
            return " FROM $tableName ${whereClause(fieldName, id, sep)}"
        }


        fun read(fieldName: String, id: Any): List<R> {
//            val req = "SELECT $selectHeader FROM $tableName $join ${whereClause(fieldName, id, and)};"
            val req = "SELECT * ${fromWhere(fieldName, id, and)};"
            return this@Persister.read(req)
                .getList { readPersisterData.evaluate(DBReadValue(this)) }
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
            write("INSERT INTO $tableName ${readPersisterData.insertList(o)}")
            readPersisterData.insertLists(o)
            tableEvent()
        }

        fun exists(fieldName: String, id: Any): Boolean {
            return this@Persister.read("SELECT EXISTS( SELECT $selectHeader ${fromWhere(fieldName,
                                                                                        id,
                                                                                        comma)});").getInt(1) != 0
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
            } catch (e:Throwable){
                throw e
            }
        }

        fun delete(id: Any) {
            delete(idName, id)
        }

        fun clear() {
            this@Persister.write("DELETE from $tableName;")
            tableEvent()
        }

        /**
         * [fieldName2Set] is the fieldName of the field, that has to be reset by [value].
         * All rows are replaced by [value], where [fieldName2Find] = [id].
         *
         * e.g. UPDATE [clazz] SET [fieldName2Set] = [value] WHERE [fieldName2Find] = [id];
         */
        fun update(fieldName2Find: String, id: Any, fieldName2Set: String, value: Any) {
            write("UPDATE $tableName SET ${fNEqualsValue(fieldName2Set, value, comma)} ${whereClause(fieldName2Find,
                                                                                                     id,
                                                                                                     comma)}")
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
                this@Persister.read(cmd(fieldName))
                    .getList { clazz.cast(getObject(1)) }
            } else {
                val readPersisterData = ReadPersisterData.create<T, Any>(clazz, this@Persister)
                val key = readPersisterData.createTableKeyData(fieldName)
                this@Persister.read(cmd(key))
                    .getList { readPersisterData.readKey(DBReadValue(this)) } as List<T>
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
            return this@Persister.read("SELECT * from $tableName;")
                .getList { readPersisterData.evaluate(DBReadValue(this)) }
        }

        /**
         * returns all keys from the current Table [clazz]
         */
        fun <T : Any> readAllKeys(): List<T> {
            return readColumn(idName) { "SELECT $selectKeyHeader from $tableName;" }
        }

        fun tableData(): TableData {
            val values = this@Persister.read("SELECT * from $tableName;")
                .getList { readPersisterData.readToString { getObject(it) } }
            return TableData(_tableName, readPersisterData.header(), values)
        }

        fun helperTables() = readPersisterData.helperTables()
        fun keyTables() = readPersisterData.keyTables()

        fun allTables() = AllTables(tableData(), helperTables(), keyTables())

        fun readAllTableData(tables: AllTables): List<R> {
            return tables.tableData.values.map { readPersisterData.evaluate(TableDataReadValue(tables, it)) }
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