package org.daiv.reflection.read

import org.daiv.reflection.common.*
import org.daiv.reflection.persister.InsertMap
import org.daiv.reflection.persister.KeyGetter
import org.daiv.reflection.persister.ReadCache
import org.daiv.reflection.plain.*
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

    class ObjectKeyToWriteImpl(val t: Any, val keyType: KeyType):ObjectKeyToWrite{
        override fun theObject() = t

        override fun itsKey() = keyType.getObject(t) as List<Any>

        override fun itsHashCode() = keyType.key!!.plainHashCodeX(itsKey())

        override fun keyToWrite(counter: Int?) = keyType.hashCodeXIfAutoKey(t, counter)

        override fun isAutoId() = keyType.isAuto()

        override fun toObjectKey(hashCounter: Int?) =
                if (isAutoId())
                    HashCodeKey(keyToWrite(hashCounter), itsHashCode(), hashCounter!!)
                else
                    PersistenceKey(keyToWrite(null))

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as ObjectKeyToWriteImpl

            if (t != other.t) return false

            return true
        }


        override fun hashCode(): Int {
            return t.hashCode()
        }

        override fun toString(): String {
            return t.toString()
        }
    }

    fun keyToWrite(t: Any): ObjectKeyToWrite {
        return ObjectKeyToWriteImpl(t, this)
    }

    fun toObjectKey(t: Any, hashCounter: Int?) = if (isAuto()) {
        val obj = getObject(t) as List<Any>
        HashCodeKey(obj, key!!.plainHashCodeX(obj), hashCounter!!)
    } else
        PersistenceKey(getObject(t) as List<Any>)

    fun objectKey(key: List<Any>, hashCounter: Int? = null) = if (isAuto())
        HashCodeKey(key, this.key!!.plainHashCodeX(key), hashCounter!!)
    else
        PersistenceKey(key)

//    fun objectKey(key: List<Any>, hashCounter: Int?): ObjectKey {
//        return if (isAuto()) {
//            PersistenceKey(listOf(plainHashCodeXIfAutoKey(key)), true, hashCounter)
//        } else {
//            PersistenceKey(key, false, null)
//        }
//    }

    /**
     * returns hashcodeX of [getObject] of [t] if this is a autoKey, [getObject] of [t] else
     */
    override fun hashCodeXIfAutoKey(t: Any, counter: Int?): List<Any> {
        propertyData.receiverType
        val obj = getObject(t)
        return key?.let {
            try {
                listOf(it.plainHashCodeX(obj), counter!!)
            } catch(t:Throwable){
                throw t
            }
        } ?: obj as List<Any>
    }

    fun plainHashCodeX(t: List<Any>): Int {
        return key?.plainHashCodeX(t)!!
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
                HashCodeKey(p.first().asList(), p.first() as Int, p[1] as Int)
            }
            return NextSize(ReadAnswer(res), ret.last().i)
        } else {
            val res: ObjectKey? = if (p.any { it == null }) null else PersistenceKey(p as List<Any>)
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

    override fun fNEqualsValue(o: Any, sep: String, keyGetter: KeyGetter): String {
        try {
            o as List<Any>
            return fields.mapIndexed { i, e ->
                val obj = o[i]
                e.fNEqualsValue(obj, sep, keyGetter)
            }
                    .joinToString(sep)
        } catch (t: Throwable) {
            throw t
        }
    }

    /**
     * if autoId, the value must already be the hashCodeX
     */
    fun autoIdFNEqualsValue(o: List<Any>, sep: String, keyGetter: KeyGetter): String {
        return fields.mapIndexed { i, e ->
            if (e is AutoKeyType) {
                e.autoIdFNEqualsValue(o[i])
            } else {
                e.fNEqualsValue(o[i], sep, keyGetter)
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

    override fun toString() = "$name - $fields"

    override fun insertObject(o: Any?, keyGetter: KeyGetter): List<InsertObject> {
        try {
            if (o == null) {
                return fields.map { it.insertObject(null, keyGetter) }
                        .flatten()
            }
            val x = o as List<Any?>
            return fields.mapIndexed { i, e -> e.insertObject(x[i], keyGetter) }
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
