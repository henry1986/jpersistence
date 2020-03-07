package org.daiv.reflection.persistence.kotlin

import org.daiv.reflection.database.mappedPersister
import org.daiv.reflection.read.Mapper
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.on
import kotlin.reflect.KClass

class ObjectMappingTest :
        Spek({
                 describe("") {
                     data class Origin(val x: Int, val y: String, val z: Int)
                     data class Mapped(val x: Int, val y: String)
                     data class Holder(val x: Int, val y: Origin)
                     data class HolderInList(val x: Int, val ys: List<Origin>)

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
                     on("test single object mapping") {
                         val x1 = Holder(5, Origin(5, "Hello", 5))
                         val x2 = Holder(6, Origin(9, "Word", 5))
                         val list = listOf(x1, x2)
                         persister.testTable(this, Holder::class, list, listOf(5), x1)
                     }
                     on("test collection object mapping") {
                         val o1 = Origin(5, "Hello", 5)
                         val o2 = Origin(9, "Word", 5)
                         val o3 = Origin(10, "hWord", 5)
                         val o4 = Origin(21, "rWord", 5)
                         val x1 = HolderInList(5, listOf(o1, o2))
                         val x2 = HolderInList(6, listOf(o2, o3, o4))
                         val list = listOf(x1, x2)
                         persister.testTable(this, HolderInList::class, list, listOf(5), x1)
                     }
                     afterGroup { persister.delete() }
                 }
             })
