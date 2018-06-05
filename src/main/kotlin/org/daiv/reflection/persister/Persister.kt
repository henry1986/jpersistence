package org.daiv.reflection.persister

import org.daiv.reflection.database.DatabaseInterface
import org.daiv.reflection.read.KeyPersisterData
import org.daiv.reflection.read.ReadPersisterData
import org.daiv.reflection.write.WritePersisterData
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import kotlin.reflect.KClass

/**
 * @author Martin Heinrich
 */
class Persister(private val statement: Statement) {

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
        println("table: $createTable")
        write(createTable)
    }

    fun insert(o: Any) {
        val createTable = WritePersisterData.create(o)
            .insert()
        println(createTable)
        write(createTable)
    }

    inner class Table<R : Any>(clazz: KClass<R>) {
        private val readPersisterData: ReadPersisterData<R> = ReadPersisterData.create(clazz)
        private fun getKey(fieldName: String, id: Any): String {
            val idPersister = KeyPersisterData.create(fieldName, id)
            return " FROM ${readPersisterData.tableName} WHERE ${idPersister.id}"
        }

        fun read(fieldName: String, id: Any): R {
            return readPersisterData.evaluate(this@Persister.read("SELECT * ${getKey(fieldName, id)};"))
        }

        fun read(id: Any): R {
            return read(readPersisterData.getIdName(), id)
        }

        fun exists(fieldName: String, id: Any): Boolean {
            return this@Persister.read("SELECT EXISTS( SELECT * ${getKey(fieldName, id)});").getInt(1) != 0
        }

        fun exists(id: Any): Boolean {
            return exists(readPersisterData.getIdName(), id)
        }

        fun delete(fieldName: String, id: Any) {
            this@Persister.write("DELETE ${getKey(fieldName, id)};")
        }

        fun delete(id: Any) {
            delete(readPersisterData.getIdName(), id)
        }
    }

}