package org.daiv.reflection.plain


internal interface IsAutoId {
    fun isAutoId(): Boolean
}

internal interface ObjectKey : IsAutoId {
    fun keys(): List<Any>

    companion object {
        val empty: ObjectKey = PersistenceKey(emptyList(), true)
    }
}

internal interface ObjectKeyToWrite : IsAutoId {
    fun theObject(): Any
    fun itsKey(): List<Any>
    fun itsHashCode(): Int
    fun keyToWrite(): List<Any>
    fun toObjectKey(): ObjectKey
}

internal data class PersistenceKey(val keys: List<Any>, val autoId: Boolean) : ObjectKey {
    override fun keys() = keys
    override fun isAutoId() = autoId
}