package org.daiv.reflection.read

import org.daiv.reflection.common.*
import org.daiv.reflection.persister.InsertMap
import org.daiv.reflection.persister.ReadCache
import org.daiv.reflection.plain.ObjectKey
import org.daiv.reflection.plain.ObjectKeyToWrite
import org.daiv.reflection.plain.PersistenceKey
import org.daiv.reflection.plain.SimpleReadObject
import org.daiv.util.recIndexed

/**
 * in case, this is an auto idField, [idFieldIfAuto] is the older idField without [key]
 */
internal class KeyType constructor(val fields: List<FieldData>,
                                   val key: KeyHashCodeable? = null,
                                   val idFieldIfAuto: KeyType? = null) : NoList {
    override val propertyData: PropertyData = KeyTypeProperty(fields)

    fun isAuto() = key != null

    override fun plainType(name: String): SimpleReadObject? = null

    override val prefix: String?
        get() = fields.first().prefix

    override fun getColumnValue(readValue: ReadValue): Any {
        return fields.first()
                .getColumnValue(readValue)
    }

    override suspend fun toStoreData(insertMap: InsertMap, objectValue: List<Any>) {
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

    fun keyToWrite(t: Any): ObjectKeyToWrite {
        return object : ObjectKeyToWrite {
            override fun theObject() = t

            override fun itsKey() = getObject(t) as List<Any>

            override fun itsHashCode() = key!!.plainHashCodeX(t)

            override fun keyToWrite() = hashCodeXIfAutoKey(t)

            override fun isAutoId() = this@KeyType.isAuto()

            override fun toObjectKey(hashCounter: Int?) = PersistenceKey(keyToWrite(), isAutoId(), hashCounter)
        }
    }

    fun toObjectKey(t: Any, hashCounter: Int?) = PersistenceKey(hashCodeXIfAutoKey(t), isAuto(), if (isAuto()) hashCounter else null)

    fun objectKey(key: List<Any>, hashCounter: Int?): ObjectKey {
        return if (isAuto()) {
            PersistenceKey(listOf(plainHashCodeXIfAutoKey(key)), true, hashCounter)
        } else {
            PersistenceKey(key, false, null)
        }
    }

    /**
     * returns hashcodeX of [getObject] of [t] if this is a autoKey, [getObject] of [t] else
     */
    override fun hashCodeXIfAutoKey(t: Any): List<Any> {
        val obj = getObject(t)
        return key?.let {
            listOf(it.plainHashCodeX(obj), 0)
        } ?: obj as List<Any>
    }

    private fun plainHashCodeXIfAutoKey(t: Any): Any {
        return key?.plainHashCodeX(t) ?: t
    }

    override fun getObject(o: Any): Any {
        if (idFieldIfAuto != null) {
            return idFieldIfAuto.getObject(o)
        }
        return super.getObject(o)
    }


    override fun subFields(): List<FieldData> {
        return fields.flatMap { it.subFields() }
    }

    fun getKeyValue(readCache: ReadCache, readValue: ReadValue, number: Int): NextSize<ReadAnswer<ObjectKey>> {
        val ret: List<NextSize<ReadAnswer<Any>>> = fields.recIndexed { i, fieldData, list ->
            val read = fieldData.getValue(readCache, readValue, list.lastOrNull()?.i ?: number, ObjectKey.empty)
            read
        }
        val p = ret.map { it.t.t }
        if (isAuto()) {
            val res: ObjectKey? = if (p.any { it == null }) null else {
                p as List<Any>
                PersistenceKey(p.first().asList(), true, p[1] as Int)
            }
            return NextSize(ReadAnswer(res), ret.last().i)
        } else {
            val res: ObjectKey? = if (p.any { it == null }) null else PersistenceKey(p as List<Any>, false, null)
            return NextSize(ReadAnswer(res), ret.last().i)
        }
    }

    override fun getValue(readCache: ReadCache, readValue: ReadValue, number: Int, key: ObjectKey): NextSize<ReadAnswer<Any>> {
        val ret: List<NextSize<ReadAnswer<Any>>> = fields.recIndexed { i, fieldData, list ->
            val read = fieldData.getValue(readCache, readValue, list.lastOrNull()?.i ?: number, ObjectKey.empty)
            read
        }
        val p = ret.map { it.t.t }
        val list = if (p.any { it == null }) null else p
        return NextSize(ReadAnswer(list), ret.last().i) as NextSize<ReadAnswer<Any>>
    }

    override fun fNEqualsValue(o: Any, sep: String): String {
        try {
            o as List<Any>
            return fields.mapIndexed { i, e ->
                val obj = o[i]
                e.fNEqualsValue(obj, sep)
            }
                    .joinToString(sep)
        }catch (t:Throwable){
            throw t
        }
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

    override fun toTableHead(nullable: Boolean): String? {
        return fields.map { it.toTableHead(nullable) }
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

    override fun insertObject(o: Any?): List<InsertObject> {
        try {
            if (o == null) {
                return fields.map { it.insertObject(null) }
                        .flatten()
            }
            val x = o as List<Any?>
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

    fun <X> onKey(f: FieldData.() -> X): X {
        return (fields.first())
                .f()
    }

    fun keyString() = fields.map { it.key() }.joinToString(", ")//fields.first().key()

    fun toPrimaryKey() = fields.joinToString(", ") { it.key() }
}
