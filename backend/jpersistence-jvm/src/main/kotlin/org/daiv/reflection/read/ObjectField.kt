package org.daiv.reflection.read

import org.daiv.reflection.common.*
import org.daiv.reflection.persister.InsertMap
import org.daiv.reflection.persister.KeyGetter
import org.daiv.reflection.persister.ReadCache
import org.daiv.reflection.plain.ObjectKey
import org.daiv.reflection.plain.SimpleReadObject

internal class ObjectField(override val propertyData: PropertyData, override val prefix: String?) : NoList {
    private val instance = propertyData.clazz.objectInstance!!

    val simpleName = propertyData.clazz.simpleName!!

    override fun fNEqualsValue(o: Any, sep: String, keyGetter: KeyGetter): String? {
        return "$prefixedName = $instance"
    }

    override fun plainType(name: String): SimpleReadObject? {
        return null
    }

    override fun getColumnValue(readValue: ReadValue): Any {
        return instance
    }

    override fun getValue(readCache: ReadCache, readValue: ReadValue, number: Int, key: ObjectKey): NextSize<ReadAnswer<Any>> {
        return NextSize(ReadAnswer(instance), number)
    }

    override fun insertObject(o: Any?, keyGetter: KeyGetter): List<InsertObject> {
        return emptyList()
    }

    override fun underscoreName(): String? {
        return prefixedName
    }


    override fun toTableHead(nullable: Boolean): String? {
        return null
    }

    override fun keySimpleType(r: Any): Any {
        return instance
    }

    override fun key(): String? {
        return null
    }

    override suspend fun toStoreData(insertMap: InsertMap, objectValue: List<Any>) {
    }

    override fun subFields(): List<FieldData> {
        return emptyList()
    }
}