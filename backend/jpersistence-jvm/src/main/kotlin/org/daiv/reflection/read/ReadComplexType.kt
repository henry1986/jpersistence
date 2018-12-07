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

import org.daiv.reflection.annotations.ManyToOne
import org.daiv.reflection.common.FieldData.ForeignKey
import org.daiv.reflection.common.FieldData.JoinName
import org.daiv.reflection.common.NoList
import org.daiv.reflection.common.PropertyData
import org.daiv.reflection.common.storeManyToOneObject
import org.daiv.reflection.persister.Persister
import java.sql.ResultSet
import kotlin.reflect.KClass

internal class ReadComplexType<R : Any, T : Any>(override val propertyData: PropertyData<R, T, T>,
                                                 private val manyToOne: ManyToOne,
                                                 private val persister: Persister,
                                                 private val persisterData: ReadPersisterData<T, Any> = ReadPersisterData.create(
                                                     propertyData.clazz,
                                                     persister)) : NoList<R, T, T> {
    private val table = persister.Table(propertyData.clazz, manyToOne.tableName)
    override fun createTableForeign() = table.persist()

    val clazz = propertyData.clazz
    override fun getColumnValue(resultSet: ResultSet) = persisterData.readKey(resultSet)

    override fun joinNames(prefix: String?, clazzSimpleName: String, keyName: String): List<JoinName> {
        return persisterData.onFields {
            joinNames(this@ReadComplexType.name(prefix), clazzSimpleName, persisterData.keyName())
        }
            .flatMap { it }
    }

    override fun keyClassSimpleType() = persisterData.keyClassSimpleType()
    override fun keySimpleType(r: R) = persisterData.keySimpleType(propertyData.getObject(r))
    override fun keyLowSimpleType(t: T) = persisterData.keySimpleType(t)

    override fun foreignKey(): ForeignKey {
        return ForeignKey(name, "${clazz.simpleName}(${persisterData.keyName()})")
    }

    override fun underscoreName(prefix: String?): String {
        return persisterData.onKey { underscoreName(this@ReadComplexType.name(prefix)) }
//            .joinToString(", ")
    }

    override fun key(prefix: String?): String {
        return persisterData.onKey { key(this@ReadComplexType.name(prefix)) }//persisterData.createTableKeyData(name(prefix))
    }

    override fun toTableHead(prefix: String?): String? {
        val name = name(prefix)
        return persisterData.onKey { toTableHead(name) }
    }

    override fun getValue(resultSet: ResultSet, number: Int, key: Any?): NextSize<T> {
        val table = persister.Table(clazz, manyToOne.tableName)
        val nextSize = persisterData.onKey { getValue(resultSet, number, key) }
        val value = table.read(nextSize.t)!!
        return NextSize(value, nextSize.i)
    }

    override fun fNEqualsValue(o: R, prefix: String?, sep: String): String {
        val objectValue = getObject(o)
        val n = name(prefix)
        return persisterData.onKey { fNEqualsValue(objectValue, n, sep) }
    }

    override fun insertObject(o: R, prefix: String?): List<InsertObject<Any>> {
        val objectValue = getObject(o)
        persisterData.storeManyToOneObject(objectValue, table)
        val n = name(prefix)
        return persisterData.onKey { insertObject(objectValue, n) }
    }
}