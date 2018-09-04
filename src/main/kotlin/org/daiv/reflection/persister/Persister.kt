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
import org.daiv.reflection.common.getList
import org.daiv.reflection.database.DatabaseInterface
import org.daiv.reflection.isPrimitiveOrWrapperOrString
import org.daiv.reflection.read.ReadPersisterData
import org.daiv.reflection.write.WritePersisterData
import org.daiv.reflection.write.WriteSimpleType
import org.daiv.util.DefaultRegisterer
import org.daiv.util.Registerer
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import kotlin.reflect.KClass
import kotlin.reflect.full.cast

/**
 * @author Martin Heinrich
 */
class Persister(private val statement: Statement,
                private val registerer: DefaultRegisterer<DBChangeListener> = DefaultRegisterer()) :
    Registerer<DBChangeListener> by registerer {

    private fun event() {
        registerer.forEach(DBChangeListener::onChange)
    }

    constructor(databaseInterface: DatabaseInterface) : this(databaseInterface.statement)

    private fun <T : Any> createTable(clazz: KClass<T>): String {
        return ReadPersisterData.create(clazz)
            .createTable()
    }

    private fun read(query: String): ResultSet {
        try {
            return statement.executeQuery(query)
        } catch (e: SQLException) {
            throw RuntimeException("query: $query", e)
        }
    }

    private fun write(query: String) {
        try {
            statement.execute(query)
        } catch (e: SQLException) {
            throw RuntimeException("query: $query", e)
        }

    }

    fun <T : Any> persist(clazz: KClass<T>) {
        val createTable = createTable(clazz)
        logger.debug { "table: $createTable" }
        write(createTable)
        event()
    }

    inner class Table<R : Any>(private val clazz: KClass<R>,
                               private val registerer: DefaultRegisterer<DBChangeListener> = DefaultRegisterer()) :
        Registerer<DBChangeListener> by registerer {

        private val readPersisterData: ReadPersisterData<R> = ReadPersisterData.create(clazz)
        private val writePersisterData: WritePersisterData<R> = WritePersisterData.create(clazz)

        private fun tableEvent() {
            registerer.forEach(DBChangeListener::onChange)
            event()
        }

        /**
         * returns "[fieldName] = [id]" for primitive Types, or the complex variant for complex types
         * for complex types, the [sep] is needed
         */
        private fun fNEqualsValue(fieldName: String, id: Any, sep: String): String {
            return if (id::class.java.isPrimitiveOrWrapperOrString()) {
                "$fieldName = ${WriteSimpleType.makeString(id)}"
            } else {
                val writePersisterData = WritePersisterData.create(id::class as KClass<Any>)
                "${writePersisterData.fNEqualsValue(id, fieldName, sep)}"
            }
        }

        private fun whereClause(fieldName: String, id: Any, sep: String): String {
            return "WHERE ${fNEqualsValue(fieldName, id, " and ")}"
        }

        private fun fromWhere(fieldName: String, id: Any, sep: String): String {
            return " FROM $tableName ${whereClause(fieldName, id, sep)}"
        }

        fun read(fieldName: String, id: Any): List<R> {
            return readPersisterData.evaluateToList(this@Persister.read("SELECT * ${fromWhere(fieldName, id, and)};"))
        }

        private val idName
            get() = readPersisterData.getIdName()

        private val tableName
            get() = readPersisterData.tableName

        fun read(id: Any): R {
            return read(idName, id)[0]
        }

        fun insert(o: R) {
            val createTable = writePersisterData.insert(o)
            logger.debug(createTable)
            write(createTable)
            tableEvent()
        }

        fun insert(o: List<R>) {
            val createTable = writePersisterData.insertList(o)
            logger.debug(createTable)
            write(createTable)
            tableEvent()
        }

        fun exists(fieldName: String, id: Any): Boolean {
            return this@Persister.read("SELECT EXISTS( SELECT * ${fromWhere(fieldName, id, comma)});").getInt(1) != 0
        }

        fun exists(id: Any): Boolean {
            return exists(idName, id)
        }

        fun delete(fieldName: String, id: Any) {
            this@Persister.write("DELETE ${fromWhere(fieldName, id, comma)};")
            tableEvent()
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


        /**
         * returns all distinct values of the column [fieldName]
         *
         * e.g. SELECT DISTINCT [fieldName] from [clazz];
         *
         * @since 0.0.8 -> group by is used instead of distinct
         */
        fun <T : Any> distinctValues(fieldName: String, clazz: KClass<T>): List<T> {

            return if (clazz.java.isPrimitiveOrWrapperOrString()) {
                this@Persister.read("SELECT $fieldName from $tableName GROUP BY $fieldName;")
                    .getList { clazz.cast(getObject(1)) }
            } else {
                val readPersisterData = ReadPersisterData.create(clazz)
                val key = readPersisterData.createTableKeyData(fieldName)
                this@Persister.read("SELECT $key from $tableName GROUP BY $key;")
                    .getList { clazz.cast(readPersisterData.evaluate(this)) }
            }
        }

        /**
         * returns all data from the current Table [clazz]
         */
        fun readAll(): List<R> {
            return this@Persister.read("SELECT * from $tableName;")
                .getList(readPersisterData::evaluate)
        }


//        fun read(fieldName: String, id: Any) : List<R>{
//            return this@Persister.read("SELECT * ${fromWhere(fieldName, id)}")
//                .getList(readPersisterData::evaluate)
//        }
    }

    companion object {
        private val comma = ","
        private val and = " and "
        private val logger = KotlinLogging.logger {}
    }
}