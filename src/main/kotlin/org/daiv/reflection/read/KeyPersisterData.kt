package org.daiv.reflection.read

import org.daiv.reflection.common.FieldDataFactory
import org.daiv.reflection.isPrimitiveOrWrapperOrString
import org.daiv.reflection.write.WriteFieldData
import org.daiv.reflection.write.WritePersisterData

internal class KeyPersisterData(val id: String) {

    companion object {
        private fun getId(prefix: String?, fields: List<WriteFieldData<Any>>): String {
            return fields.joinToString(separator = ") and ( ",
                                       prefix = "(",
                                       postfix = ")",
                                       transform = { f -> f.name(prefix) + " = " + f.insertValue() })
        }

        fun <T : Any> create(r: ReadPersisterData<T>, o: Any): KeyPersisterData {
            if (o::class.java.isPrimitiveOrWrapperOrString()) {
                return KeyPersisterData("${r.getIdName()} = $o")
            }
            return KeyPersisterData(getId(r.getIdName(), FieldDataFactory.fieldsWrite(o)))
        }
    }
}


