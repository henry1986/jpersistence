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

import org.daiv.reflection.annotations.TableData
import org.daiv.reflection.isPrimitiveOrWrapperOrStringOrEnum
import org.daiv.reflection.persister.Persister
import org.daiv.reflection.persister.Persister.HelperTable
import org.daiv.reflection.persister.Persister.Table
import org.daiv.reflection.read.InsertObject
import org.daiv.reflection.read.KeyType
import org.daiv.reflection.read.NextSize
import org.daiv.reflection.read.ReadPersisterData
import kotlin.reflect.KClass

private fun <T : Any> T.checkDBValue(objectValue: T): T {
    if (this != objectValue) {
        val msg = "values are not the same -> " +
                "databaseValue: $this vs manyToOne Value: $objectValue"
        throw RuntimeException(msg)
    }
    return objectValue
}

internal fun <T : Any> ReadPersisterData<T, Any>.storeManyToOneObject(objectValue: T, table: Table<T>) {
    if (objectValue::class.isPrimitiveOrWrapperOrStringOrEnum()) {
        return
    }
    val key = getKey(objectValue)
    table.read(key)?.let { it.checkDBValue(objectValue) } ?: run {
        table.insert(objectValue)
    }
}

internal fun <T : Any> ReadPersisterData<T, Any>.storeManyToOneObject(objectValues: List<T>, table: Table<T>) {
    if (objectValues.isEmpty()) {
        return
    }
    if (objectValues.first()::class.isPrimitiveOrWrapperOrStringOrEnum()) {
        return
    }
    val l = objectValues.map { objectValue ->
        val x = table.read(getKey(objectValue))
                ?.let { it.checkDBValue(objectValue) }
        if (x == null) {
            objectValue
        } else {
            null
        }
    }
            .filterNotNull()
            .distinct()
    if (!l.isEmpty()) {
        table.insert(l)
    }
}

internal interface FieldCollection<R : Any, S : Any, T : Any, X : Any> {
    /**
     * returns "[fieldName] = [id]" for primitive Types, or the complex variant
     * for complex types, the [sep] is needed
     */
    fun fNEqualsValue(o: S, sep: String): String

    fun whereClause(id: S, sep: String): String {
            return "WHERE ${fNEqualsValue(id, sep)}"
    }

    /**
     * this method creates the string for the sql command "CREATE TABLE".
     *
     * @param prefix
     * a possible prefix for the variables name. Null, if no prefix
     * is wanted.
     * @return the string for the "CREATE TABLE" command or null, if not needed
     */
    fun toTableHead(): String?

    fun getColumnValue(readValue: ReadValue): Any
    fun getValue(readValue: ReadValue, number: Int, key: Any?): NextSize<X>
    fun insertObject(o: T): List<InsertObject>
    fun underscoreName(): String?
}

internal interface FieldReadable<R : Any, S : Any> {
    fun getObject(o: R): S
}

internal interface HashCodeable<S : Any> : FieldReadable<Any, S> {
    fun hashCodeX(t: Any): Int
}

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


    fun keySimpleType(r: R): Any
    fun keyLowSimpleType(t: T): Any

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
    fun name(prefix: String?, name: String = this.name) = prefix?.let { "${it}_$name" } ?: name

//    fun <R : Any> onMany2One(onManyToOne: (ManyToOne) -> R, noManyToOne: () -> R): R {
//        return property.findAnnotation<ManyToOne>()?.let(onManyToOne) ?: run(noManyToOne)
//    }


    /**
     * gets the [property] value of [o]
     */
    override fun getObject(o: R): S {
        try {
            val x = propertyData.getObject(o)
            return x
        } catch (t: Throwable) {
            throw RuntimeException("could not read property $propertyData  of $o", t)
        }

    }


    fun createTable()
    fun insertLists(r: List<R>)
    fun deleteLists(keySimpleType: Any)
    fun clearLists()
    fun createTableForeign()

    fun key(): String

    data class ForeignKey(val name: String, val clazzReference: String) {
        fun sqlMethod() = "FOREIGN KEY ($name) references $clazzReference"
    }

//    fun foreignKey(): ForeignKey?

    data class JoinName(val name: String, val clazzSimpleName: String, val keyName: String) {
        fun join() = "${clazzSimpleName} as $name ON $name = $name.${keyName}"
    }

//    fun joinNames(clazzSimpleName: String, keyName: String): List<JoinName>

    fun size() = 1

    fun storeManyToOneObject(t: List<T>)
    fun toStoreObjects(objectValue: T): List<ToStoreManyToOneObjects>
    fun persist()
    fun subFields(): List<FieldData<Any, Any, Any, Any>>

//    fun makeString(any: R): String
}

internal data class ToStoreManyToOneObjects(val field: FieldData<*, *, *, *>, val any: Any)

internal interface NoList<R : Any, S : Any, T : Any> : FieldData<R, S, T, S> {
    override fun createTable() {}
    override fun createTableForeign() {}
    override fun insertLists(r: List<R>) {}
    override fun deleteLists(keySimpleType: Any) {}
    override fun clearLists() {}
}

internal interface CollectionFieldData<R : Any, S : Any, T : Any, X : Any> : FieldData<R, S, T, X> {
    override fun subFields(): List<FieldData<Any, Any, Any, Any>> = emptyList()
    override fun keySimpleType(r: R) = throw RuntimeException("a collection cannot be a key")
    override fun keyLowSimpleType(t: T) = t
    override fun key() = throw RuntimeException("a collection cannot be a key")
    override fun toTableHead() = null
    override fun toStoreObjects(objectValue: T): List<ToStoreManyToOneObjects> = emptyList()

    override fun insertObject(o: T): List<InsertObject> = listOf()
    //    override fun foreignKey() = null
//    override fun joinNames(clazzSimpleName: String, keyName: String): List<FieldData.JoinName> =
//            emptyList()

    override fun getColumnValue(readValue: ReadValue) = throw RuntimeException("a collection cannot be a key")

    override fun underscoreName() = null
    override fun size() = 0
    override fun storeManyToOneObject(t: List<T>) {}
    override fun persist() {}
}
