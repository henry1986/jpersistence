package org.daiv.reflection.persistence.kotlin

import org.daiv.reflection.read.KeyPersisterData
import org.daiv.reflection.read.ReadPersisterData
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import kotlin.test.assertEquals

class KeyPersisterDataTest :
    Spek({
             given("a object") {
                 on("keyvalue") {
                     it("simple type") {
                         data class SimpleObject(val i: Int)
                         val create = KeyPersisterData.create(ReadPersisterData.create(SimpleObject::class).getIdName(), 5)
                         val id = create.id
                         assertEquals("i = 5", id)
                     }
                 }
             }
         })