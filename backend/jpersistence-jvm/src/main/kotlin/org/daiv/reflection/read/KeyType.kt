package org.daiv.reflection.read

import org.daiv.reflection.common.*
import org.daiv.reflection.persister.InsertMap
import org.daiv.reflection.persister.ReadCache

/**
 * in case, this is an auto idField, [idFieldIfAuto] is the older idField without [key]
 */
internal class KeyType constructor(val fields: List<FieldData<Any, Any, Any, Any>>,
                                   val key: KeyHashCodeable? = null,
                                   val idFieldIfAuto: KeyType? = null) : NoList<Any, List<Any>, Any> {
    override val propertyData: PropertyData<Any, List<Any>, Any> = KeyTypeProperty(fields)

    fun isAuto() = key != null

    override val prefix: String?
        get() = fields.first().prefix

    override fun getColumnValue(readValue: ReadValue): Any {
        return fields.first()
                .getColumnValue(readValue)
    }

    override fun numberOfKeyFields(): Int = fields.size

    override fun toStoreData(insertMap: InsertMap, objectValue: List<Any>) {
    }

    override fun keySimpleType(r: Any): Any {
        return fields.first()
                .keySimpleType(r)
    }

    fun isKeyType(list: List<Any>): Boolean {
        if (idFieldIfAuto != null) {
            return idFieldIfAuto.isKeyType(list)
        }
        if (list.size != fields.size) {
            return false
        }
        return fields.mapIndexed { i, e -> e.isType(list[i]) }
                .all { it }
    }

    override fun keyValue(o: Any) = (idFieldIfAuto?.propertyData ?: propertyData).getObject(o)

    override fun key(): String {
        return fields.map { it.key() }
                .joinToString(", ")
    }

    /**
     * returns hashcodeX of [getObject] of [t] if this is a autoKey, [getObject] of [t] else
     */
    override fun hashCodeXIfAutoKey(t: Any): List<Any> {
        val obj = getObject(t)
        return key?.plainHashCodeX(obj)?.asList() ?: obj
    }

    override fun plainHashCodeXIfAutoKey(t: Any): Any {
        return key?.plainHashCodeX(t) ?: t
    }

    override fun getObject(o: Any): List<Any> {
        if (idFieldIfAuto != null) {
            return idFieldIfAuto.getObject(o)
        }
        return super.getObject(o)
    }

    override fun toStoreObjects(objectValue: Any): List<ToStoreManyToOneObjects> {
        return emptyList()
    }

    override fun persist() {
        fields.forEach { it.persist() }
    }

    override fun subFields(): List<FieldData<Any, Any, Any, Any>> {
        return fields.flatMap { it.subFields() }
    }

    private fun read(i: Int, readCache: ReadCache,readValue: ReadValue, number: Int, ret: NextSize<List<Any>>): NextSize<List<Any>> {
        if (i < fields.size) {
            val read = fields[i].getValue(readCache, readValue, number, emptyList())
            return read(i + 1, readCache, readValue, read.i, NextSize(ret.t + read.t, read.i))
        }
        return ret
    }

    override fun getValue(readCache: ReadCache, readValue: ReadValue, number: Int, key: List<Any>): NextSize<List<Any>> {
        return read(0, readCache, readValue, number, NextSize(emptyList(), number))
//        return fields.first()
//                .getValue(readValue, number, key)
    }

    override fun fNEqualsValue(o: List<Any>, sep: String): String {
        return fields.mapIndexed { i, e ->
            val obj = o[i]
//            if(e is AutoKeyType){
//                e.plainHashCodeXIfAutoKey(obj)
//            } else{
            e.fNEqualsValue(obj, sep)
//            }
        }
                .joinToString(sep)
    }

    /**
     * if autoId, the value must already be the hashCodeX
     */
    fun autoIdFNEqualsValue(o: List<Any>, sep: String): String {
        return fields.mapIndexed { i, e ->
            if (e is AutoKeyType) {
                e.autoIdFNEqualsValue(o[i])
            } else {
                e.fNEqualsValue(o[i], sep)
            }
        }
                .joinToString(sep)
    }


//    override fun getObject(o: Any): Any {
//        return fields.first()
//                .getObject(o)
//    }

    override fun toTableHead(): String? {
        return fields.map { it.toTableHead() }
                .joinToString(", ")
    }

    override fun copyTableName(): Map<String, String> {
        return fields.flatMap {
            it.copyTableName()
                    .entries
        }
                .map { it.key to it.value }
                .toMap()
    }

    override fun insertObject(o: Any): List<InsertObject> {
        try {
            val x = o as List<Any>
            return fields.mapIndexed { i, e -> e.insertObject(x[i]) }
                    .flatten()
        } catch (t: Throwable) {
            throw t
        }
    }

    override fun underscoreName(): String? {
        return fields.map { it.underscoreName() }
                .joinToString(", ")
    }

    val fieldName: String = fields.map { it.name(null) }
            .joinToString(" AND ")
//            = fields.first()
//            .name(null)

    fun simpleType(r: Any) = fields.first().keySimpleType(r)

    fun name() = fields.first()
            .key()

    fun columnName() = fields.first().prefixedName

    fun <X> onKey(f: FieldData<Any, Any, Any, Any>.() -> X): X {
        return (fields.first())
                .f()
    }

    fun keyString() = fields.map { it.key() }.joinToString(", ")//fields.first().key()

    fun toPrimaryKey() = fields.joinToString(", ") { it.key() }
}
