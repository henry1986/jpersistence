package org.daiv.reflection.read

import org.daiv.reflection.common.*
import org.daiv.reflection.persister.InsertMap
import org.daiv.reflection.persister.KeyGetter
import org.daiv.reflection.persister.ReadCache
import org.daiv.reflection.plain.ObjectKey
import org.daiv.reflection.plain.SimpleReadObject

internal class ManyToManyType(override val propertyData: PropertyData, override val prefix: String?) :NoList {
    override fun toTableHead(nullable: Boolean): String? {
        TODO("Not yet implemented")
    }

    override fun keySimpleType(r: Any): Any {
        TODO("Not yet implemented")
    }

    override fun key(): String? {
        TODO("Not yet implemented")
    }

    override suspend fun toStoreData(insertMap: InsertMap, objectValue: List<Any>) {
        TODO("Not yet implemented")
    }

    override fun subFields(): List<FieldData> {
        TODO("Not yet implemented")
    }

    override fun fNEqualsValue(o: Any, sep: String, keyGetter: KeyGetter): String? {
        TODO("Not yet implemented")
    }

    override fun plainType(name: String): SimpleReadObject? {
        TODO("Not yet implemented")
    }

    override fun getColumnValue(readValue: ReadValue): Any {
        TODO("Not yet implemented")
    }

    override fun getValue(
        readCache: ReadCache,
        readValue: ReadValue,
        number: Int,
        key: ObjectKey
    ): NextSize<ReadAnswer<Any>> {
        TODO("Not yet implemented")
    }

    override fun insertObject(o: Any?, keyGetter: KeyGetter): List<InsertObject> {
        TODO("Not yet implemented")
    }

    override fun underscoreName(): String? {
        TODO("Not yet implemented")
    }
}