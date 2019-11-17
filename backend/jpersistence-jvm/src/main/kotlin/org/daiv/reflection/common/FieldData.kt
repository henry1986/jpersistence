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

package org.daiv.reflection.common

import org.daiv.reflection.persister.InsertMap
import org.daiv.reflection.persister.InsertRequest
import org.daiv.reflection.persister.Persister.HelperTable
import org.daiv.reflection.persister.ReadCache
import org.daiv.reflection.plain.SimpleReadObject
import org.daiv.reflection.read.InsertObject
import org.daiv.reflection.read.KeyType
import org.daiv.reflection.read.NextSize

private fun <T : Any> T.checkDBValue(objectValue: T): T {
    if (this != objectValue) {
        val msg = "values are not the same -> " +
                "databaseValue: $this vs manyToOne Value: $objectValue"
        throw RuntimeException(msg)
    }
    return objectValue
}

internal data class ReadAnswer<T : Any?>(val t: T?, val exists: Boolean = true)

internal interface FieldCollection<R : Any, S : Any, T : Any, X : Any> {
    /**
     * returns "[fieldName] = [id]" for primitive Types, or the complex variant
     * for complex types, the [sep] is needed
     */
    fun fNEqualsValue(o: S, sep: String): String

    fun whereClause(id: S, sep: String): String {
        return "WHERE ${fNEqualsValue(id, sep)}"
    }

    fun plainType(name: String): SimpleReadObject?

    fun getColumnValue(readValue: ReadValue): Any
    fun getValue(readCache: ReadCache, readValue: ReadValue, number: Int, key: List<Any>): NextSize<ReadAnswer<X>>
    fun insertObject(o: T?): List<InsertObject>
    fun underscoreName(): String?
}

internal interface FieldReadable<R : Any, S : Any> {
    fun getObject(o: R): S
}

internal interface HashCodeable<S : Any> : FieldReadable<Any, S> {
    fun hashCodeX(t: Any): Int
    fun plainHashCodeX(t: Any): Int
}

fun toPrefixedName(prefix: String?, name: String) = prefix?.let { "${it}_$name" } ?: name

/**
 * [R] is the type of the receiver of the property
 * [T] is the generic type of the PropertyData
 * [S] is the type of the value returned by the getObject method
 * [X] is the transformed value of [S] -> necessary for example with the map to list converter
 */
internal interface FieldData<R : Any, S : Any, T : Any, X : Any> : FieldCollection<R, S, T, X>, FieldReadable<R, S> {
    val propertyData: PropertyData<R, S, T>

    val name get() = propertyData.name
    val prefix: String?
    val prefixedName
        get() = name(prefix)

//    fun tableName(): List<String> = emptyList()

    fun numberOfKeyFields(): Int

    fun isType(a: Any): Boolean {
        return propertyData.clazz == a::class
    }

    /**
     * this method creates the string for the sql command "CREATE TABLE".
     *
     * @param prefix
     * a possible prefix for the variables name. Null, if no prefix
     * is wanted.
     * @return the string for the "CREATE TABLE" command or null, if not needed
     */
    fun toTableHead(nullable: Boolean = propertyData.isNullable): String?

    fun keyValue(o: R) = propertyData.getObject(o)

    fun keySimpleType(r: R): Any

    fun collectionElementName(prefix: String?, i: Int): String {
        val indexedName = "${name}_$i"
        return prefix?.let { "${it}_$indexedName" } ?: "$indexedName"
    }

//    fun getGenericListType(): KClass<Any> {
//        return property.returnType.arguments.first().type!!.classifier as KClass<Any>
//    }


    /**
     * builds the name of the field in the database
     *
     * @param prefix
     * if the name of the field in the database shall have a prefix,
     * it is to be given here. null otherwise.
     * @return the name of the field in the database
     */
    fun name(prefix: String?, name: String = this.name) = toPrefixedName(prefix, name)

//    fun <R : Any> onMany2One(onManyToOne: (ManyToOne) -> R, noManyToOne: () -> R): R {
//        return property.findAnnotation<ManyToOne>()?.let(onManyToOne) ?: run(noManyToOne)
//    }

    /**
     * gets the [propertyData] value of [o]
     */
    override fun getObject(o: R): S {
        try {
            val x = propertyData.getObject(o)
            return x
        } catch (t: Throwable) {
            throw RuntimeException("could not read property $propertyData ${propertyData.clazz} and receiverType: ${propertyData.receiverType} of $o",
                                   t)
        }
    }

    /**
     * returns hashcodeX if this is a autoKey, [getObject] of [t] else
     */
    fun hashCodeXIfAutoKey(t: R): S {
        return getObject(t)
    }

    fun plainHashCodeXIfAutoKey(t: T): T {
        return t
    }

    fun deleteLists(key: List<Any>)
    fun clearLists()
    fun createTableForeign(tableName: Set<String>): Set<String>

    fun key(): String

    fun size() = 1

    fun copyTableName(): Map<String, String>
    fun copyData(map: Map<String, String>, request: (String, String) -> String)

    fun toStoreObjects(objectValue: T): List<ToStoreManyToOneObjects>
    suspend fun toStoreData(insertMap: InsertMap, objectValue: List<R>)
    fun persist()
    fun subFields(): List<FieldData<Any, Any, Any, Any>>

    fun onIdField(idField: KeyType)

    fun dropHelper()

//    fun makeString(any: R): String
}

internal data class ToStoreManyToOneObjects(val field: FieldData<*, *, *, *>, val any: Any)

internal interface NoList<R : Any, S : Any, T : Any> : FieldData<R, S, T, S> {
    override fun createTableForeign(tableName: Set<String>) = tableName
    override fun deleteLists(key: List<Any>) {}
    override fun clearLists() {}
    override fun copyTableName() = mapOf(prefixedName to prefixedName)
    override fun copyData(map: Map<String, String>, request: (String, String) -> String) {}
    override fun onIdField(idField: KeyType) {}
    override fun dropHelper() {}
}

internal interface CollectionFieldData<R : Any, S : Any, T : Any, X : Any> : FieldData<R, S, T, X> {
    val helperTable: HelperTable

    override fun dropHelper() {
        helperTable.dropTable()
    }

    override fun plainType(name: String): SimpleReadObject? = null


    override fun numberOfKeyFields() = 0

    override fun subFields(): List<FieldData<Any, Any, Any, Any>> = emptyList()
    override fun keySimpleType(r: R) = throw RuntimeException("a collection cannot be a key")
    override fun key() = throw RuntimeException("a collection cannot be a key")
    override fun toTableHead(nullable: Boolean) = null
    override fun toStoreObjects(objectValue: T): List<ToStoreManyToOneObjects> = emptyList()

    override fun insertObject(o: T?): List<InsertObject> = listOf()
    //    override fun foreignKey() = null
//    override fun joinNames(clazzSimpleName: String, keyName: String): List<FieldData.JoinName> =
//            emptyList()

    override fun getColumnValue(readValue: ReadValue) = throw RuntimeException("a collection cannot be a key")

    override fun underscoreName() = null
    override fun size() = 0
    override fun persist() {}
    override fun copyTableName() = emptyMap<String, String>()

    override fun copyData(map: Map<String, String>, request: (String, String) -> String) {
        val nextName = map["&newTableName"] ?: helperTable._tableName
        val currentName = map["&oldTableName"] ?: helperTable._tableName
        if (nextName == currentName) {
            val tmpName = "tmp_$currentName"
            helperTable.persistWithName(tmpName)
            helperTable.copyData(map, currentName, tmpName) { request(currentName, it) }
            helperTable.onlyDropMaster(currentName)
            helperTable.rename(tmpName, currentName)
        } else {
            helperTable.persistWithName(nextName)
            helperTable.copyData(map, currentName, nextName) { request(currentName, it) }
            helperTable.onlyDropMaster(currentName)
        }
    }
}

internal interface SimpleTypes<R : Any, T : Any> : NoList<R, T, T> {
    override fun toTableHead(nullable: Boolean): String {
        val notNull = if (nullable) "" else " NOT NULL"
        return "$prefixedName $typeName$notNull"
    }

    val typeName: String

    override fun numberOfKeyFields(): Int = 1

    override fun toStoreObjects(objectValue: T): List<ToStoreManyToOneObjects> = emptyList()
    override suspend fun toStoreData(insertMap: InsertMap, objectValue: List<R>) {}

    override fun subFields(): List<FieldData<Any, Any, Any, Any>> = emptyList()

    override fun persist() {}
    override fun getColumnValue(resultSet: ReadValue) = resultSet.getObject(1)!!

    override fun keySimpleType(r: R) = propertyData.getObject(r)
    override fun key() = prefixedName


    override fun underscoreName() = name(prefix)

    override fun insertObject(t: T?): List<InsertObject> {
        return listOf(object : InsertObject {
            override fun insertValue(): String {
                return t?.let { makeString(it) } ?: "null"
            }

            override fun insertHead(): String {
                return prefixedName
            }
        })
    }

    fun makeString(t: T): String
    fun makeStringOnEqualsValue(t: T): String = makeString(t)

    override fun fNEqualsValue(o: T, sep: String): String {
        return "$prefixedName = ${makeStringOnEqualsValue(o)}"
//        return "$prefixedName = ${makeString(o)}"
    }


}