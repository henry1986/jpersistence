package org.daiv.reflection.persistence.kotlin

import kotlin.test.Test
import org.daiv.reflection.annotations.MoreKeys
import org.daiv.reflection.database.mappedPersister
import org.daiv.reflection.read.Mapper
import kotlin.reflect.KClass
import kotlin.test.AfterTest
import kotlin.test.assertEquals

open class ObjectMappingTest {
    data class Origin(val x: Int, val y: String, val z: Int)
    data class Mapped(val x: Int, val y: String)
    data class Holder(val x: Int, val y: Origin)

    @MoreKeys(2)
    data class HolderInList(val x: Int, val origin: Origin, val ys: List<Origin>)

    val mapper = object : Mapper<Origin, Mapped> {

        override val mapped: KClass<Mapped> = Mapped::class
        override val origin: KClass<Origin> = Origin::class

        override fun map(origin: Origin): Mapped {
            return Mapped(origin.x, origin.y)
        }

        override fun backwards(mapped: Mapped): Origin {
            return Origin(mapped.x, mapped.y, 5)
        }
    }
    val persister = mappedPersister("ObjectMappingTest.db", listOf(mapper))

    @AfterTest
    fun afterTest(){
        persister.delete()
    }
}
class DirectMapping : ObjectMappingTest(){
    val x1 = Origin(1, "Hello", 5)
    val x2 = Origin(2, "Word", 5)
    val list = listOf(x1, x2)
    @Test
    fun test() {
        persister.testTable( Origin::class, list, listOf(1), x1)
    }
}
class TestSingleObjectMapping : ObjectMappingTest(){
    val x1 = Holder(5, Origin(5, "Hello", 5))
    val x2 = Holder(6, Origin(9, "Word", 5))
    val list = listOf(x1, x2)
    @Test
    fun test() {
        persister.testTable( Holder::class, list, listOf(5), x1)
    }
}
class TestCollectionObjectMapping : ObjectMappingTest(){
    val o1 = Origin(5, "Hello", 5)
    val o2 = Origin(9, "Word", 5)
    val o3 = Origin(10, "hWord", 5)
    val o4 = Origin(21, "rWord", 5)
    val x1 = HolderInList(5, o1, listOf(o1, o2))
    val x2 = HolderInList(6, o4, listOf(o2, o3, o4))
    val list = listOf(x1, x2)
    @Test
    fun test() {
        persister.testTable(HolderInList::class, list, listOf(5, o1), x1) {
            @Test
            fun testFirst() {
                val first = first()
                assertEquals(x1, first)
            }
        }
    }
}
