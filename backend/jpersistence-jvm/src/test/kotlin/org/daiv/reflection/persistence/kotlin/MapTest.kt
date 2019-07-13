package org.daiv.reflection.persistence.kotlin

import org.daiv.immutable.utils.persistence.annotations.DatabaseWrapper
import org.daiv.reflection.persister.Persister
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on

class MapTest :
        Spek({
                 data class SpecialMapObject1(val id: Int)
                 data class SpecialMapObject2(val id: Double)
                 data class ToStore(val id: Int, val map: Map<String, List<Any>>)

                 val database = DatabaseWrapper.create("MapTest.db")
                 database.open()
                 describe("complex Collection") {
                     val persister = Persister(database)
//                     val table = persister.Table(ToStore::class)
                     on("test") {
                         it("persist") {
//                             table.persist()
                         }
                     }
                     afterGroup { database.delete() }
                 }
             })