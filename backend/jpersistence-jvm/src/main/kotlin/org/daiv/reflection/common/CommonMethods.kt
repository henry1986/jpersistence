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

import org.daiv.reflection.annotations.IFaceForList
import org.daiv.reflection.isEnum
import org.daiv.reflection.isPrimitiveOrWrapperOrString
import org.daiv.reflection.read.*
import java.sql.ResultSet
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.createType

fun <R : Any> ResultSet.getList(method: ResultSet.() -> R): List<R> {
    val mutableList = mutableListOf<R>()
    while (next()) {
        mutableList.add(method())
    }
    return mutableList.toList()
}

fun <T : Any> KClass<T>.tableName() = "${this.java.simpleName}"

fun <T : Any> T?.asList() = listOfNotNull(this)

internal class ReadValue constructor(val resultSet: ResultSet) {
    fun getObject(number: Int): Any? = resultSet.getObject(number)
}

internal fun KType.createWithoutInterface(clazz: KClass<*>, persisterProvider: PersisterProvider, prefix: String?): FieldData {
    return when {
        clazz.java.isPrimitiveOrWrapperOrString() -> ReadSimpleType(SimpleTypeProperty(this, clazz.simpleName!!), prefix)
        clazz.isEnum() -> EnumType(SimpleTypeProperty(this, clazz.simpleName!!), prefix)
        clazz.isNoMapAndNoListAndNoSet() ->
            ReadComplexType(SimpleTypeProperty(this, clazz.simpleName!!), clazz.moreKeys(), clazz.including(), persisterProvider, prefix)
        else -> throw RuntimeException("type unknown: $clazz in $this")
    }
}

internal fun <T : Any> KType.toMap(interf: IFaceForList?, depth: Int, createFct: (String, KClass<*>) -> T): Map<String, T> {
    interf ?: throw RuntimeException(" no interface annotation for $this")
    val i = interf.ifaceForObject.firstOrNull { it.depth == depth }
            ?: throw RuntimeException("for depth $depth there is nothing configured in $this")
    val map = i.classesNames.map {
        val name = InterfaceField.nameOfClass(it)
        name to createFct(name, it)
    }
            .toMap()
    return map
}

internal fun KType.toLowField(persisterProvider: PersisterProvider, depth: Int, prefix: String?, interf: IFaceForList? = null): FieldData {
    val clazz = this.classifier as KClass<Any>
    return when {
        clazz.java.isPrimitiveOrWrapperOrString() -> ReadSimpleType(SimpleTypeProperty(this, clazz.simpleName!!), prefix)
        clazz.isEnum() -> EnumType(SimpleTypeProperty(this, clazz.simpleName!!), prefix)
        clazz.java.isInterface && clazz.isNoMapAndNoListAndNoSet() -> {
            val map = toMap(interf, depth) { name, it ->
                PossibleImplementation(name, it.createType().createWithoutInterface(it, persisterProvider, prefix))
            }
            InterfaceField(SimpleTypeProperty(this, InterfaceField.nameOfClass(clazz)), prefix, map)
        }
        clazz.isNoMapAndNoListAndNoSet() ->
            ReadComplexType(SimpleTypeProperty(this, clazz.simpleName!!), clazz.moreKeys(), clazz.including(), persisterProvider, prefix)
        clazz == List::class -> InnerMapType(SimpleListTypeProperty(this),
                                             depth,
                                             interf,
                                             persisterProvider,
                                             prefix,
                                             listConverter,
                                             listBackConverter)
        clazz == Map::class -> InnerMapType(SimpleMapTypeProperty(this), depth, interf, persisterProvider, prefix)
        clazz == Set::class -> InnerSetType(SimpleSetProperty(this), depth, persisterProvider, prefix)
        else -> {
            throw RuntimeException("this: $this not possible")
        }
    }
}


//class TableDataReadValue constructor(val tables: AllTables, val values: List<String>) : ReadValue {
//    override fun getObject(number: Int, clazz: KClass<Any>): Any {
//        val x = values[number-1]
//        try {
//            return when (clazz) {
//                Boolean::class -> x.toBoolean()
//                String::class -> x
//                Int::class -> x.toInt()
//                Double::class -> x.toDouble()
//                else -> throw RuntimeException("unknow type: $clazz")
//            }
//        } catch (e: NumberFormatException) {
//            throw e
//        }
//    }
//
//    override fun <T : Any> read(table: Table<T>, t: Any): T {
//        return table.readTableData(tables.copy(tableData = tables.keyTables.find { it.tableName == table._tableName }!!),
//                                   t)
//    }
//
//    override fun <T:Any>helperTable(table: Table<T>, fieldName: String, key: Any): List<T> {
//        val tableData = tables.helper.find { it.tableName == table._tableName }!!
//        return table.readTableData(tables, tableData, fieldName, key)
//
//    }
//
//}