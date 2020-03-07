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
import kotlin.reflect.KClass


interface Mapper<ORIGIN : Any, MAPPED : Any> {
    val origin: KClass<ORIGIN>
    val mapped: KClass<MAPPED>
    fun map(origin: ORIGIN): MAPPED
    fun backwards(mapped: MAPPED): ORIGIN
}

internal class DefaultMapper<T : Any>(override val origin: KClass<T>) : Mapper<T, T> {
    override val mapped: KClass<T>
        get() = origin

    override fun map(origin: T) = origin
    override fun backwards(mapped: T) = mapped
}

internal class ReadComplexType constructor(_propertyData: OtherClassPropertyData,
                                           val moreKeys: MoreKeys,
                                           val including: Including,
                                           val persisterProvider: PersisterProvider,
                                           override val prefix: String?) : NoList {
    override val propertyData: PropertyData = persisterProvider.mapProviderClazz(_propertyData)
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
            val obj = persisterProvider.map(getObject(it))
            if (propertyData.isNullable && obj == null) {
                return
            }
            if (including.include) {
                return
            }
            if (obj == null) {
                throw NullPointerException()
            }
            val key = persisterData.key.keyToWrite(obj)

//            val key = persisterData.key.hashCodeXIfAutoKey(obj)
            insertMap.nextTask(table, key) { keyToWrite ->
                table.readPersisterData.putInsertRequests(table, insertMap, listOf(obj), listOf(keyToWrite))
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
        val read = persisterProvider.backmap(readCache.readNoNull(table, nextSize.t.t))
        return NextSize(ReadAnswer(read), nextSize.i)
    }

    override fun fNEqualsValue(objectValue: Any, sep: String, keyGetter: KeyGetter): String? {
//        val objectKey = persisterData.mapObjectToKey(objectValue, keyCreator, table)
        val objectKey = keyGetter.keyForObject(table, persisterData.key.keyToWrite(objectValue))
                ?: return null
        return persisterData.key.fNEqualsValue(objectKey.keys(), sep, keyGetter)
    }

    override fun insertObject(objectValue: Any?, keyGetter: KeyGetter): List<InsertObject> {
        if (objectValue == null) {
            return persisterData.key.insertObject(null, keyGetter)
        }
//        val objectKey = persisterData.mapObjectToKey(objectValue, table)
        val key = persisterData.key.keyToWrite(persisterProvider.map(objectValue) ?: throw NullPointerException())
        return persisterData.key.insertObject(keyGetter.keyForObjectFromCache(table, key)?.keys()
                                                      ?: throw RuntimeException("did not find key for $table and $key")
                                              , keyGetter)
    }
}
