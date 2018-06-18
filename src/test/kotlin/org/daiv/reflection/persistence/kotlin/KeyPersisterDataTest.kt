/*
 * Copyright (c) 2018. Martin Heinrich - All Rights Reserved
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 */

package org.daiv.reflection.persistence.kotlin

import org.daiv.reflection.read.KeyPersisterData
import org.daiv.reflection.read.ReadPersisterData
import org.daiv.reflection.write.WritePersisterData
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import kotlin.test.assertEquals

class KeyPersisterDataTest :
    Spek({
             given("a object") {
                 on("keyvalue") {
                     data class SimpleObject(val i: Int)
                     it("simple type") {
                         val create = KeyPersisterData.create(ReadPersisterData.create(SimpleObject::class).getIdName(), 5)
                         val id = create.id
                         assertEquals("i = 5", id)
                     }
                     it("test on WritePersisterData"){
                         val simpleObject = SimpleObject(1)
                         val writePersisterData = WritePersisterData.create(simpleObject)
                         val fNEqualsValue = writePersisterData.fNEqualsValue(null, ", ")
                         assertEquals("i = 1", fNEqualsValue)
                     }
                     data class ComplexObject(val x : SimpleObject, val s : String)
                     it("test on ComplexObject"){
                         val c = ComplexObject(SimpleObject(5), "Hallo")
                         val writer = WritePersisterData.create(c)
                         val fNEqualsValue = writer.fNEqualsValue(null, ", ")
                         assertEquals("x_i = 5, s = \"Hallo\"", fNEqualsValue)
                     }
                     data class SuperComplex(val z:ComplexObject, val s : String)
                     it("test on SuperComplexObject"){
                         val c = SuperComplex(ComplexObject(SimpleObject(5), "Hallo"), "wow")
                         val writer = WritePersisterData.create(c)
                         val fNEqualsValue = writer.fNEqualsValue(null, ", ")
                         assertEquals("z_x_i = 5, z_s = \"Hallo\", s = \"wow\"", fNEqualsValue)
                         println(fNEqualsValue)
                     }
                     it("test on simple"){

                     }
                 }
             }
         })