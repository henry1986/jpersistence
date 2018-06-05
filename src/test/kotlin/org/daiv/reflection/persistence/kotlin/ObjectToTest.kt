package org.daiv.reflection.persistence.kotlin

import org.daiv.immutable.utils.persistence.annotations.DatabaseWrapper
import org.daiv.reflection.database.DatabaseInterface
import org.daiv.reflection.persister.Persister
import org.daiv.reflection.read.ReadPersisterData
import org.daiv.reflection.write.WritePersisterData
import kotlin.test.assertEquals

class ObjectToTest(private val o: Any,
                   tableString: String,
                   insertString: String,
                   private val testName: String,
                   private val key: Any) {
    private val tableCreateString: String = createTableString(o::class.simpleName!!, tableString)
    private val insertString: String = createInsertString(o::class.simpleName!!, insertString)
    private val d: DatabaseInterface = DatabaseWrapper.create("PersisterTest$testName.db")
    private val p by lazy { Persister(d) }
    private val r = ReadPersisterData.create(o::class)
    private val w = WritePersisterData.create(o)

    fun open() {
        d.open()
    }

    fun beforeReadFromDatabase() {
        d.statement.execute(r.createTable())
        d.statement.execute(w.insert())
    }

    fun checkCreateTable() {
        val createTable = r.createTable()
        println(createTable)
        assertEquals(tableCreateString, createTable)
    }

    fun checkInsert() {
        val insert = w.insert()
        assertEquals(insertString, insert)
        println(insert)
    }

    fun checkReadData() {
//        val query = "SELECT * FROM ${r.tableName} WHERE ${r.getIdName()} = $key;"
        assertEquals(o, p.Table(o::class).read(key))
    }

    override fun toString(): String {
        return testName
    }

    fun delete() {
        d.delete()
    }
}

fun createTableString(objectName: String, values: String): String {
    return "CREATE TABLE IF NOT EXISTS $objectName $values;"
}

fun createInsertString(objectName: String, values: String): String {
    return "INSERT INTO $objectName $values;"
}
