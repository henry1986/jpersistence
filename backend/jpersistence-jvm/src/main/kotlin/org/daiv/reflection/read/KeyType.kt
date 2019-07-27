package org.daiv.reflection.read

import org.daiv.reflection.common.*

internal class KeyType constructor(val fields: List<FieldData<Any, Any, Any, Any>>) : NoList<Any, List<Any>, Any> {
    override val propertyData: PropertyData<Any, List<Any>, Any> = KeyTypeProperty(fields)

    override val prefix: String?
        get() = fields.first().prefix

    override fun getColumnValue(readValue: ReadValue): Any {
        return fields.first()
                .getColumnValue(readValue)
    }

    override fun keySimpleType(r: Any): Any {
        return fields.first()
                .keySimpleType(r)
    }

    override fun keyLowSimpleType(t: Any): Any {
        return fields.first()
                .keyLowSimpleType(t)
    }

    override fun key(): String {
        return fields.map { it.key() }
                .joinToString(", ")
    }

    override fun storeManyToOneObject(t: List<Any>) {
//        fields.first()
//                .storeManyToOneObject(t)
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

    private fun read(i: Int, readValue: ReadValue, number: Int, key: Any?, ret: NextSize<List<Any>>): NextSize<List<Any>> {
        if (i < fields.size) {
            val read = fields[i].getValue(readValue, number, key)
            return read(i + 1, readValue, read.i, key, NextSize(ret.t + read.t, read.i))
        }
        return ret
    }

    override fun getValue(readValue: ReadValue, number: Int, key: Any?): NextSize<List<Any>> {
        return read(0, readValue, number, key, NextSize(emptyList(), number))
//        return fields.first()
//                .getValue(readValue, number, key)
    }

    override fun fNEqualsValue(o: List<Any>, sep: String): String {
        return fields.mapIndexed { i, e ->
            val obj = o[i]
            e.fNEqualsValue(obj, sep)
        }
                .joinToString(sep)
    }

    fun autoIdFNEqualsValue(o: Any, sep: String): String {
        return fields.mapIndexed { i, e ->
            if (e is AutoKeyType) {
                o as List<Any>
                e.autoIdFNEqualsValue(o.first())
            } else {
                e.fNEqualsValue(o, sep)
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
