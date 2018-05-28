package org.daiv.reflection.persister

import org.daiv.immutable.utils.persistence.annotations.DatabaseInterface
import org.daiv.reflection.read.KeyPersisterData
import org.daiv.reflection.read.ReadPersisterData
import org.daiv.reflection.write.WritePersisterData
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import kotlin.reflect.KClass

class Persister(private val statement: Statement) {

    constructor(databaseInterface: DatabaseInterface) : this(databaseInterface.statement)

    private fun <T : Any> createTable(clazz: KClass<T>): String {
        return ReadPersisterData.create<T>(clazz)
            .createTable()
    }

    private fun read(query: String): ResultSet {
        try {
            return statement.executeQuery(query)
        } catch (e: SQLException) {
            throw RuntimeException(e)
        }

    }

    private fun write(query: String) {
        try {
            statement.execute(query)
        } catch (e: SQLException) {
            throw RuntimeException(e)
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

    fun <T : Any> read(clazz: KClass<T>, id: Any): T {
        val persisterData = ReadPersisterData.create(clazz)
        val idPersister = KeyPersisterData.create(persisterData, id)
        val query = "SELECT * FROM ${persisterData.tableName} WHERE ${idPersister.id};"
        println(query)
        val execute = read(query)
        return persisterData.read(execute).t
    }

}