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

import org.daiv.reflection.common.*
import org.daiv.reflection.persister.Persister

internal class ComplexSameTableType<R : Any, T : Any> constructor(override val propertyData: PropertyData<R, T, T>,
                                                                  val persisterProvider: PersisterProvider,
                                                                  override val prefix: String?,
                                                                  parentTableName: String?,
                                                                  persister: Persister) :
        NoList<R, T, T> {
    override fun toStoreObjects(objectValue: T): List<ToStoreManyToOneObjects> = emptyList()
    private val builder = FieldDataFactory(persisterProvider,
                                           propertyData.clazz,
                                           prefixedName,
                                           parentTableName,
                                           persister).fieldsRead()
    private val persisterData: ReadPersisterData<T, Any> = ReadPersisterData(builder.idField!!,
                                                                             builder.fields as List<FieldData<T, Any, Any, Any>>,
                                                                             method = ReadPersisterData.readValue(propertyData.clazz))

    override fun subFields() = persisterData.fields as List<FieldData<Any, Any, Any, Any>>

    override fun storeManyToOneObject(t: List<T>) {}

    override fun persist() {}

    override fun getColumnValue(resultSet: ReadValue) = persisterData.readKey(resultSet)

    override fun keySimpleType(r: R) = r

    override fun insertObject(o: T): List<InsertObject> {
        return persisterData.onFields { this.insertObject(this.getObject(o)) }
                .flatten()
    }

    override fun fNEqualsValue(o: T, sep: String): String {
        return persisterData.onFields { fNEqualsValue(getObject(o), sep) }
                .joinToString(sep)
    }

    override fun toTableHead(): String? {
        return persisterData.onFields {
            toTableHead()
        }
                .joinToString(", ")
    }


    override fun key(): String {
        return persisterData.onFields { key() }
                .joinToString(", ")//persisterData.createTableKeyData(name(prefix))
//        return persisterData.createTableKeyData(prefix)
    }

    //    override fun foreignKey(): FieldData.ForeignKey? {
//        return null
//    }
//
//    override fun joinNames(clazzSimpleName: String, keyName: String): List<FieldData.JoinName> {
//        return listOf()
//    }

    override fun underscoreName(): String {
        return persisterData.onFields { underscoreName() }
                .joinToString(", ")
    }

    override fun getValue(readValue: ReadValue, number: Int, key: List<Any>): NextSize<T> {
        return persisterData.read(readValue, number)
    }

//    override fun header() = persisterData.onFields { header() }.flatten()

    override fun size() = persisterData.onFields { size() }.sum()

//    override fun keyTables() = persisterData.keyTables()
//
//    override fun helperTables() = persisterData.helperTables()
}