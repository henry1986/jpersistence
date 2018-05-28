package org.daiv.reflection.persistence.kotlin

import org.daiv.reflection.common.FieldDataFactory
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import kotlin.test.assertEquals

class FieldDataFactoryTest :
    Spek({
             given("check fields order") {
                 on("") {
                     it("") {
                         data class SimpleObject(val x: Int, val a: Int)

                         val s = SimpleObject(5, 4)
                         val clazz = s::class
                         val x = FieldDataFactory.fieldsRead(clazz)
                         assertEquals("x",x[0].name())
                         assertEquals("a",x[1].name())
                     }
                 }
             }

         })