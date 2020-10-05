package org.daiv.reflection.persistence.kotlin

import kotlin.test.Test
import mu.KotlinLogging
import org.daiv.reflection.persister.Persister
import kotlin.reflect.full.memberProperties
import kotlin.test.AfterTest
import kotlin.test.assertEquals

data class SimpleObject(val x: Int, val y: String)
data class KeySimpleObject(val x: Int, val y: String)
data class InnerKeySimpleObject(val x: Int, val y: String)
data class ListObject(val id: Int, val list: List<List<SimpleObject>>)
data class ListMapObject(val id: Int, val list: List<Map<InnerKeySimpleObject, SimpleObject>>)
data class SetObject(val id: Int, val list: Set<SimpleObject>)
data class MapObject(val id: Int, val map: Map<KeySimpleObject, Map<InnerKeySimpleObject, SimpleObject>>)

open class MapTest {

    val logger = KotlinLogging.logger {}
    val s0 = SimpleObject(0, "Hello")
    val s1 = SimpleObject(1, "Hello")
    val s2 = SimpleObject(2, "World")
    val k0 = KeySimpleObject(0, "Hello")
    val k1 = KeySimpleObject(1, "Hello")
    val k2 = KeySimpleObject(2, "World")
    val i0 = InnerKeySimpleObject(0, "Hello")
    val i1 = InnerKeySimpleObject(1, "Hello")
    val i2 = InnerKeySimpleObject(2, "World")
    val m = MapObject(
        0, mapOf(
            k0 to mapOf(i0 to s0, i1 to s1),
            k1 to mapOf(i0 to s1, i1 to s1)
        )
    )
    val ps = MapObject::class.memberProperties.toList()
    val p = ps[1]
    val t = p.returnType.arguments[1].type!!

    init {
        if (t.classifier == Map::class && p.returnType.arguments[0].type!!.classifier == KeySimpleObject::class) {
            if (t.arguments[1].type!!.classifier == SimpleObject::class) {
                val theMap = p.get(m) as Map<*, *>
                theMap.map { entry ->
                    val value = entry.value

                }
                logger.trace { "p: ${p.get(m)}" }
            }
        }
    }

    val persister = Persister("MapTest.db")
    @AfterTest
    fun afterTest() {
        persister.delete()
    }
}

class TestMap : MapTest() {
    val table = persister.Table(MapObject::class)
    init {
        table.persist()
    }
    @Test
    fun persistMap() {
        val list = listOf(
            MapObject(
                0, mapOf(
                    k0 to mapOf(i0 to s0, i1 to s1),
                    k1 to mapOf(i0 to s1, i1 to s1)
                )
            ),
            MapObject(
                1, mapOf(
                    k1 to mapOf(i0 to s0, i1 to s2),
                    k2 to mapOf(i2 to s1, i1 to s1)
                )
            )
        )
        table.insert(list)
        val all = table.readAll()
        assertEquals(list, all)
    }
}

class TestList : MapTest() {
    val table = persister.Table(ListObject::class)
    init {
        table.persist()
    }
    @Test
    fun persistList() {
        val list = listOf(
            ListObject(
                0, listOf(
                    listOf(s0, s1),
                    listOf(s2)
                )
            ),
            ListObject(
                1, listOf(
                    listOf(s0, s2),
                    listOf(s1, s2)
                )
            )
        )
        table.insert(list)
        val read = table.readAll()
        assertEquals(list, read)
    }
}

class TestListMap : MapTest() {
    val table = persister.Table(ListMapObject::class)
    init {
        table.persist()
    }
    @Test
    fun persistListMap() {
        val list = listOf(
            ListMapObject(
                0, listOf(
                    mapOf(i1 to s0, i2 to s1),
                    mapOf(i1 to s2)
                )
            ),
            ListMapObject(
                1, listOf(
                    mapOf(i1 to s0, i2 to s2),
                    mapOf(i0 to s1, i1 to s2)
                )
            )
        )
        table.insert(list)
        val read = table.readAll()
        assertEquals(list, read)
    }
}

class TestSet : MapTest() {
    val table = persister.Table(SetObject::class)
    init {
        table.persist()
    }
    @Test
    fun persistSet() {
        val list = listOf(
            SetObject(0, setOf(s0, s1)),
            SetObject(1, setOf(s0, s2))
        )

        table.insert(list)
        val read = table.readAll()
        assertEquals(list, read)
    }
}
