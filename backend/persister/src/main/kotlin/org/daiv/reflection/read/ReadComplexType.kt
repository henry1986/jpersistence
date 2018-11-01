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

import org.daiv.reflection.common.FieldData
import org.daiv.reflection.persister.Persister
import java.sql.ResultSet
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1

internal class ReadComplexType<R : Any, T : Any>(override val property: KProperty1<R, T>,
                                                 private val persister: Persister) :
    FieldData<R, T> {

    private val persisterData = ReadPersisterData.create(property.returnType.classifier as KClass<T>, persister)

    override fun key(prefix: String?): String {
        return persisterData.createTableKeyData(name(prefix))
    }

    override fun toTableHead(prefix: String?): String {
        return onMany2One({
                              val name = name(prefix)
                              persisterData.onKey { toTableHead(name) }
                          }) { persisterData.createTableString(name(prefix)) }
    }

    override fun getValue(resultSet: ResultSet, number: Int): NextSize<T> {
        return onMany2One({
                              val table = persister.Table(property.returnType.classifier as KClass<T>)
                              val nextSize = persisterData.onKey { getValue(resultSet, number) }
                              val value = table.read(nextSize.t)!!
                              NextSize(value, nextSize.i)
                          }) { persisterData.read(resultSet, number) }
    }

    override fun fNEqualsValue(o: R, prefix: String?, sep: String): String {
        return onMany2One({
                              val objectValue = getObject(o)
                              val n = name(prefix)
                              persisterData.onKey { fNEqualsValue(objectValue, n, sep) }
                          }) { persisterData.fNEqualsValue(getObject(o), name(prefix), sep) }
    }

    override fun insertObject(o: R, prefix: String?): List<InsertObject<Any>> {
        return onMany2One({
                              val objectValue = getObject(o)
                              storeManyToOneObject(persisterData, objectValue, persister)
                              val n = name(prefix)
                              persisterData.onKey { insertObject(objectValue, n) }
                          }
        ) {
            persisterData.insertObject(getObject(o), name(prefix))
        }
    }
}