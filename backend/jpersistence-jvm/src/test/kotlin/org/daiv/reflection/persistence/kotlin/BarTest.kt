package org.daiv.reflection.persistence.kotlin

import org.daiv.immutable.utils.persistence.annotations.DatabaseWrapper
import org.daiv.reflection.persister.Persister
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import kotlin.test.assertEquals

class BarTest
    : Spek({
               data class Bar(val x: Int, val s: String)

               val database = DatabaseWrapper.create("BarTest.db")
               describe("BarTest") {
                   database.open()
                   val persister = Persister(database)
                   on("from to") {
                       val list = (0 until 100).map { Bar(it, " - $it - ") }
                               .toList()
                       val table  =persister.Table(Bar::class)
                       table.persist()
                       it("from to") {
                           table.insert(list)
                           val read = table.read(5, 10)
                           assertEquals(list.subList(5, 11), read)
                       }
                   }
                   afterGroup { database.delete() }
               }
           })