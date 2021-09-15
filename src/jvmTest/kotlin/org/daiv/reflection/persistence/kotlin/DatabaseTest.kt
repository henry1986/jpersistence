package org.daiv.reflection.persistence.kotlin

import org.daiv.reflection.database.DatabaseHandler
import org.daiv.reflection.persister.Persister
import org.junit.Test
import java.sql.ResultSet

class DatabaseTest {

    data class MyTable(val x: Int, val y: String)

//    @Test
//    fun test(){
//        val p = Persister("test.db")
//        val t = p.Table(MyTable::class)
//        t.persist()
//        t.insert(MyTable(5, "Hello"))
//        t.insert(MyTable(6, "World"))
//    }

    @Test
    fun test2() {
        val d = DatabaseHandler("test.db")

        val r = d.statement.executeQuery("select * from MyTable;")
        r.next()
        val x = r.getObject(1)
        val y = r.getObject(2)
        r.next()
        val y2 = r.getString(2)
        println("x: $x")
        println("y: $y")
        println("y2: $y2")
    }
}