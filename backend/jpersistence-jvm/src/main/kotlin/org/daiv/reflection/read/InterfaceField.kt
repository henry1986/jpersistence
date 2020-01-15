package org.daiv.reflection.read

import org.daiv.reflection.common.*
import org.daiv.reflection.persister.InsertMap
import org.daiv.reflection.persister.KeyGetter
import org.daiv.reflection.persister.ReadCache
import org.daiv.reflection.plain.ObjectKey
import org.daiv.reflection.plain.SimpleReadObject
import org.daiv.util.recIndexed
import kotlin.reflect.KClass
import kotlin.reflect.full.createType

internal data class PossibleImplementation(val key: String, val fieldData: FieldData)

internal class InterfaceField constructor(override val propertyData: PropertyData,
                                          override val prefix: String?,
                                          val possibleClasses: Map<String, PossibleImplementation>) : NoList {


    companion object {
        fun nameOfClass(clazz: KClass<*>) = clazz.simpleName!!
        fun nameOfObj(obj: Any) = obj::class.simpleName!!
    }

    private val readSimpleType = ReadSimpleType(SimpleTypeProperty(String::class.createType(), "${name}_InterfaceFieldClassName"), prefix)

    override fun toTableHead(nullable: Boolean): String? {
        return (possibleClasses.map { it.value.fieldData.toTableHead(true) }.filterNotNull()
                + readSimpleType.toTableHead(nullable)).joinToString(", ")
    }

    override fun keySimpleType(r: Any): Any {
        val obj = propertyData.getObject(r)
        val name = nameOfObj(obj)
        val fieldData = possibleClasses[name] ?: throw RuntimeException()
        if (fieldData.fieldData.isType(obj)) {
            return fieldData.fieldData.keySimpleType(r)
        } else {
            throw RuntimeException()
        }
    }

    private fun <T> onAll(block: FieldData.() -> T): List<T> {
        return (possibleClasses.map { it.value.fieldData.block() } + readSimpleType.block())
    }

    private fun <T : Any?> onAllFlat(block: FieldData.() -> List<T>): List<T> {
        return (possibleClasses.flatMap { it.value.fieldData.block() } + readSimpleType.block())
    }

    override fun createTableForeign(tableName: Set<String>): Set<String> {
        return possibleClasses.flatMap { it.value.fieldData.createTableForeign(tableName) }
                .toSet()
    }

    override fun key(): String {
        return onAll { key() }.filterNotNull()
                .joinToString(", ")
    }

    private fun error(name: String) = "it's not possible to store an object of type $name " +
            "in interface ${propertyData.clazz.simpleName} of class ${propertyData.receiverType?.simpleName}" +
            " - possible solution: Use @IFaceForObject annotation and declare $name in ${propertyData.receiverType?.simpleName}"

    override suspend fun toStoreData(insertMap: InsertMap, objectValue: List<Any>) {
        val res = objectValue.map { it to getObject(it) }
                .filterNot { propertyData.isNullable && it.second == null }
                .groupBy { nameOfObj(it.second) }
                .map { it.key to it.value.map { it.first } }.toMap()
        res.map {
            val possibleImplementation = possibleClasses[it.key]
                    ?: throw RuntimeException(error(it.key))
            possibleImplementation.fieldData.toStoreData(insertMap, it.value)
        }
    }

    override fun subFields(): List<FieldData> {
        return onAllFlat { subFields() }
    }

    override fun fNEqualsValue(o: Any, sep: String, keyGetter: KeyGetter): String? {
        val name = nameOfObj(o)
        val p = possibleClasses[name]!!
        if (p.fieldData is ObjectField) {
            return readSimpleType.fNEqualsValue(p.fieldData.simpleName, sep, keyGetter)
        }
        return p.fieldData.fNEqualsValue(o, sep, keyGetter)
    }

    override fun plainType(name: String): SimpleReadObject? {
        return possibleClasses.map { it.value.fieldData.plainType(name) }
                .find { it != null }
    }

    override fun getColumnValue(readValue: ReadValue): Any {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getValue(readCache: ReadCache, readValue: ReadValue, number: Int, key: ObjectKey): NextSize<ReadAnswer<Any>> {
//        val name = readSimpleType.getValue(readCache, readValue, number, key).t.t as String
//        possibleClasses[name]!!.fieldData.getValue(readCache, readValue, number, key)
        val res: List<Pair<String, NextSize<ReadAnswer<Any>>>> = possibleClasses.toList()
                .recIndexed { i, pair, list ->
                    pair.first to pair.second.fieldData.getValue(readCache, readValue, list.lastOrNull()?.second?.i ?: number, key)
                }
        val textNextSize = readSimpleType.getValue(readCache, readValue, res.last().second.i, key)
        if (textNextSize.t.t == null) {
            return NextSize(ReadAnswer(null) as ReadAnswer<Any>, textNextSize.i)
        }
        val name = textNextSize.t.t as String
//        val name = res.last().second.t.t as String
        return NextSize(res.toMap()[name]!!.t, textNextSize.i)
    }

    override fun insertObject(o: Any?, keyGetter: KeyGetter): List<InsertObject> {
        if (o == null) {
            return onAllFlat { insertObject(null, keyGetter) }
        }
        val name = nameOfObj(o)
        return possibleClasses.flatMap {
            if (it.key == name) it.value.fieldData.insertObject(o, keyGetter) else it.value.fieldData.insertObject(null, keyGetter)
        } + readSimpleType.insertObject(name, keyGetter)
    }

    override fun underscoreName(): String? {
        return onAll { underscoreName() }.joinToString(", ")
    }

}