package org.daiv.reflection.read

import org.daiv.reflection.common.FieldDataFactory
import org.daiv.reflection.isPrimitiveOrWrapperOrString
import org.daiv.reflection.write.WriteFieldData

internal class KeyPersisterData(val id: String) {

    companion object {
        private fun getId(prefix: String?, fields: List<WriteFieldData<Any>>): String {
            return fields.joinToString(separator = ") and ( ",
                                       prefix = "(",
                                       postfix = ")",
                                       transform = { f -> f.name(prefix) + " = " + f.insertValue() })
        }

        private fun makeString(o: Any): String {
            return if (o::class == String::class) "\"$o\"" else o.toString()
        }

        fun create(fieldName: String, o: Any): KeyPersisterData {
            if (o::class.java.isPrimitiveOrWrapperOrString()) {
                return KeyPersisterData("$fieldName = ${makeString(o)}")
            }
            return KeyPersisterData(getId(fieldName, FieldDataFactory.fieldsWrite(o)))
        }
    }
}


