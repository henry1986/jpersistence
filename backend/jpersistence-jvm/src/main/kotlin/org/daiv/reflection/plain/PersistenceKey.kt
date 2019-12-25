package org.daiv.reflection.plain


internal interface IsAutoId {
    fun isAutoId(): Boolean
}

internal interface ObjectKey : IsAutoId {
    fun keys(): List<Any>
    fun hashCounter(): Int

    companion object {
        val empty: ObjectKey = PersistenceKey(emptyList(), true, 0)
    }
}

internal interface ObjectKeyToWrite : IsAutoId {
    fun theObject(): Any
    fun itsKey(): List<Any>
    fun itsHashCode(): Int
    fun keyToWrite(): List<Any>
    fun toObjectKey(hashCounter: Int?): ObjectKey
}

internal data class PersistenceKey(val keys: List<Any>, val autoId: Boolean, val counter: Int?) : ObjectKey {
    override fun keys() = if (isAutoId()) keys + hashCounter() else keys
    override fun isAutoId() = autoId
    override fun hashCounter() = counter!!
}