package org.daiv.reflection.persistence.kotlin

import org.daiv.reflection.annotations.ObjectOnly
import org.daiv.reflection.persister.Persister
import org.junit.After
import org.junit.Test
import kotlin.test.assertEquals

class SingletonObjectTest {

    interface MyInterface {
        fun myMethod(i: Int) {

        }
    }

    object MyObject : SingletonObjectTest.MyInterface
    object MyObject2 : SingletonObjectTest.MyInterface

    data class MyClass(val x: Int, @ObjectOnly val myInterface: MyInterface)
    data class MyListClass(val x: Int, @ObjectOnly val myInterface: List<MyInterface>)
    data class MySetClass(val x: Int, @ObjectOnly val myInterface: Set<MyInterface>)

    val p = Persister("SingletonObjectTest.db")

    @Test
    fun test() {
        val table = p.Table(MyClass::class)
        table.persist()
        val obj = MyClass(5, MyObject)
        table.insert(obj)
        val read = table.read(5)
        assertEquals(obj, read)
    }
    @Test
    fun testList() {
        val table = p.Table(MyListClass::class)
        table.persist()
        val obj = MyListClass(5, listOf(MyObject, MyObject2))
        table.insert(obj)
        val read = table.read(5)
        assertEquals(obj, read)
    }
    @Test
    fun testSet() {
        val table = p.Table(MySetClass::class)
        table.persist()
        val obj = MySetClass(5, setOf(MyObject, MyObject2))
        table.insert(obj)
        val read = table.read(5)
        assertEquals(obj, read)
    }

    @After
    fun afterTest(){
        p.delete()
    }
}