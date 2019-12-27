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

import org.daiv.reflection.annotations.Including
import org.daiv.reflection.annotations.MoreKeys
import org.daiv.reflection.common.*
import org.daiv.reflection.persister.*
import org.daiv.reflection.plain.ObjectKey
import org.daiv.reflection.plain.SimpleReadObject

internal class ReadComplexType constructor(override val propertyData: PropertyData,
                                           val moreKeys: MoreKeys,
                                           val including: Including,
                                           val persisterProvider: PersisterProvider,
                                           override val prefix: String?) : NoList {
    val providerKey = ProviderKey(propertyData.clazz, prefixedName)

    init {
        persisterProvider.register(providerKey)
    }

    private val persisterData
        get() = persisterProvider.readPersisterData(providerKey)

    private val table
        get() = persisterProvider.table(providerKey.clazz) as Persister.Table<Any>

    override fun subFields(): List<FieldData> = persisterData.fields

    override suspend fun toStoreData(insertMap: InsertMap, objectValue: List<Any>) {
        objectValue.forEach {
            val obj = getObject(it)
            if (propertyData.isNullable && obj == null) {
                return
            }
            if (including.include) {
                return
            }
            val key = persisterData.key.keyToWrite(obj)

//            val key = persisterData.key.hashCodeXIfAutoKey(obj)
            insertMap.nextTask(table, key) {
                table.readPersisterData.putInsertRequests(table._tableName, insertMap, listOf(obj), listOf(it))
            }
        }
    }

    override fun plainType(name: String): SimpleReadObject? {
        return persisterData.fields.asSequence()
                .map { it.plainType(name) }
                .dropWhile { it == null }
                .takeWhile { it != null }
                .lastOrNull()
    }


    override fun createTableForeign(tableNames: Set<String>): Set<String> {
        if (including.include) {
            return tableNames
        }
        if (!tableNames.contains(this.table._tableName)) {
            return table.persistWithName(checkName = tableNames)
        }
        return tableNames
    }

    val clazz = propertyData.clazz
    override fun getColumnValue(resultSet: ReadValue) = persisterData.readKey(resultSet)

    override fun keySimpleType(r: Any) = persisterData.keySimpleType(propertyData.getObject(r))


    override fun underscoreName(): String {
        return persisterData.key.underscoreName()!!
    }

    override fun key(): String {
        return persisterData.key.keyString()//persisterData.createTableKeyData(prefixedName)
    }

    override fun toTableHead(nullable: Boolean): String? {
        return persisterData.key.toTableHead(nullable || propertyData.isNullable)
    }

    override fun copyTableName(): Map<String, String> {
        return persisterData.key.copyTableName()
    }

    override fun getValue(readCache: ReadCache, readValue: ReadValue, number: Int, key: ObjectKey): NextSize<ReadAnswer<Any>> {
        val nextSize = persisterData.key.getKeyValue(readCache, readValue, number)
        if (nextSize.t.t == null) {
            return NextSize(ReadAnswer(null) as ReadAnswer<Any>, nextSize.i)
        }
        if (including.include) {
            val x = nextSize.t.t.keys()
            val list = table.readPersisterData.fields.mapIndexed { i, e -> ReadFieldValue(x[i], e) }
            val n = NextSize(list, nextSize.i)
            val r = ReadPersisterData.readValue(clazz)
            return n.transform { b -> ReadAnswer(r(b)) }
        }
        val read = readCache.readNoNull(table, nextSize.t.t)
        return NextSize(ReadAnswer(read), nextSize.i)
    }

    override fun fNEqualsValue(objectValue: Any, sep: String, keyCreator: KeyCreator): String {
        val objectKey = persisterData.mapObjectToKey(objectValue, keyCreator, table)
        return persisterData.key.fNEqualsValue(objectKey.keys(), sep, keyCreator)
    }

    override fun insertObject(objectValue: Any?, keyCreator: KeyCreator): List<InsertObject> {
        if (objectValue == null) {
            return persisterData.key.insertObject(null, keyCreator)
        }
        val objectKey = persisterData.mapObjectToKey(objectValue, keyCreator, table)
        return persisterData.key.insertObject(objectKey.keys(), keyCreator)
    }
}
