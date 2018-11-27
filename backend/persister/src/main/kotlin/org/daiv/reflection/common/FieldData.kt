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

import org.daiv.reflection.isPrimitiveOrWrapperOrString
import org.daiv.reflection.persister.Persister
import org.daiv.reflection.read.InsertObject
import org.daiv.reflection.read.NextSize
import org.daiv.reflection.read.ReadPersisterData
import java.sql.ResultSet
import kotlin.reflect.KClass

internal fun <T : Any> ReadPersisterData<T, Any>.storeManyToOneObject(objectValue: T,
                                                                      persister: Persister,
                                                                      tableName: String) {
    if (objectValue::class.java.isPrimitiveOrWrapperOrString()) {
        return
    }
    val key = getKey(objectValue)
    val table = persister.Table(objectValue::class as KClass<T>, tableName)
    table.read(key)?.let { dbValue ->
        if (dbValue != objectValue) {
            val msg = "values are not the same -> " +
                    "databaseValue: $dbValue vs manyToOne Value: $objectValue"
            throw RuntimeException(msg)
        }
    } ?: run {
        table.insert(objectValue)
    }
}

internal interface FieldData<R : Any, S : Any, T : Any> {
    val propertyData: PropertyData<R, S, T>

    val name get() = propertyData.name

    fun keyClassSimpleType(): KClass<Any>
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
    fun name(prefix: String?) = prefix?.let { "${it}_$name" } ?: name

//    fun <R : Any> onMany2One(onManyToOne: (ManyToOne) -> R, noManyToOne: () -> R): R {
//        return property.findAnnotation<ManyToOne>()?.let(onManyToOne) ?: run(noManyToOne)
//    }

    /**
     * gets the [property] value of [o]
     */
    fun getObject(o: R) = propertyData.getObject(o)

    fun insertObject(o: R, prefix: String?): List<InsertObject<Any>>

    fun fNEqualsValue(o: R, prefix: String?, sep: String): String

    /**
     * this method creates the string for the sql command "CREATE TABLE".
     *
     * @param prefix
     * a possible prefix for the variables name. Null, if no prefix
     * is wanted.
     * @return the string for the "CREATE TABLE" command or null, if not needed
     */
    fun toTableHead(prefix: String?): String?

    fun createTable()
    fun insertLists(keySimpleType: Any, r: R)

    fun key(prefix: String?): String

    data class ForeignKey(val name: String, val clazzReference: String) {
        fun sqlMethod() = "FOREIGN KEY ($name) references $clazzReference"
    }

    fun foreignKey(): ForeignKey?

    data class JoinName(val name: String, val clazzSimpleName: String, val keyName: String) {
        fun join() = "${clazzSimpleName} as $name ON $name = $name.${keyName}"
    }

    fun joinNames(prefix: String?, clazzSimpleName: String, keyName: String): List<JoinName>

    fun underscoreName(prefix: String?): String

    fun getValue(resultSet: ResultSet, number: Int, key: Any?): NextSize<S>
}

internal interface NoList<R : Any, S : Any, T : Any> : FieldData<R, S, T> {
    override fun createTable() {}
    override fun insertLists(keySimpleType: Any, r: R) {}
}

internal interface CollectionFieldData<R : Any, S : Any, T : Any> : FieldData<R, S, T> {
    override fun keyClassSimpleType() = throw RuntimeException("a collection cannot be a key")
    override fun keySimpleType(r: R) = throw RuntimeException("a collection cannot be a key")
    override fun keyLowSimpleType(t: T) = throw RuntimeException("a collection cannot be a key")
    override fun key(prefix: String?) = throw RuntimeException("a collection cannot be a key")
    override fun toTableHead(prefix: String?) = null
    override fun insertObject(o: R, prefix: String?): List<InsertObject<Any>> = listOf()
    override fun foreignKey() = null
    override fun joinNames(prefix: String?, clazzSimpleName: String, keyName: String): List<FieldData.JoinName> =
        emptyList()

    override fun underscoreName(prefix: String?) = ""

}