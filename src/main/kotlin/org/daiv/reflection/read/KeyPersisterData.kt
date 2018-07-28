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

package org.daiv.reflection.read

import org.daiv.reflection.common.FieldDataFactory
import org.daiv.reflection.isPrimitiveOrWrapperOrString
import org.daiv.reflection.write.WriteFieldData

//internal class KeyPersisterData(val id: String) {
//
//    companion object {
//        private fun getId(prefix: String?, fields: List<WriteFieldData<Any>>): String {
//            return fields.joinToString(separator = ") and ( ",
//                                       prefix = "(",
//                                       postfix = ")",
//                                       transform = { f -> f.name(prefix) + " = " + f.insertValue() })
//        }
//
//        private fun makeString(o: Any): String {
//            return if (o::class == String::class) "\"$o\"" else o.toString()
//        }
//
//        fun create(fieldName: String, o: Any): KeyPersisterData {
//            if (o::class.java.isPrimitiveOrWrapperOrString()) {
//                return KeyPersisterData("$fieldName = ${makeString(o)}")
//            }
//            return KeyPersisterData(getId(fieldName, FieldDataFactory.fieldsWrite(o)))
//        }
//    }
//}


