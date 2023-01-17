package org.daiv.reflection.persistence.kotlin

import org.daiv.reflection.annotations.MoreKeys
import org.daiv.reflection.database.persister
import org.junit.Test
import kotlin.test.AfterTest
import kotlin.test.assertEquals

class ReadCacheTest {
    @MoreKeys(1, true)
    data class ListHolder(val l:List<Int>, val s:String)

    val persister = persister("ReadCacheTest.db")

    @Test
    fun test(){
        val table = persister.Table(ListHolder::class)
        table.persist()
        val lHolder = ListHolder(listOf(1,2), "Hello")
        table.insert(lHolder)
        persister.deleteAndRestart()
        persister.resetReadCache()
        table.persist()
        table.insert(lHolder)
        val read = table.readAll()
        assertEquals(lHolder, read.first())
    }

    @AfterTest
    fun afterTest(){
        persister.delete()
    }
}