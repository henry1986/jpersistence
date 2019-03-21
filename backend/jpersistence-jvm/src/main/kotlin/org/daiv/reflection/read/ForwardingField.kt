package org.daiv.reflection.read

import org.daiv.reflection.common.FieldData
import org.daiv.reflection.common.PropertyData

internal class ForwardingField(override val propertyData: PropertyData<Any, Any, Any>,
                               private val fieldData: FieldData<Any, Any, Any>) : FieldData<Any, Any, Any> by fieldData {

    override fun insertObject(o: Any): List<InsertObject<Any>> {
//        return listOf(object : InsertObject<Any> {
//            override fun insertValue(): String {
//                return ReadSimpleType.makeString(t)
//            }
//
//            override fun insertHead(): String {
//                return prefixedName
//            }
//        })
        return fieldData.insertObject(fieldData.getObject(o))
    }

    override fun fNEqualsValue(o: Any, sep: String): String {
        return fieldData.fNEqualsValue(o, sep)
    }

    override fun getObject(o: Any): Any {
        return propertyData.getObject(o)
    }
}