package org.daiv.reflection.plain


internal interface IsAutoId {
    fun isAutoId(): Boolean
}

internal interface ObjectKey : IsAutoId {
    fun keys(): List<Any>
    fun hashCounter(): Int

    companion object {
        val empty: ObjectKey = PersistenceKey(emptyList())
    }
}

internal interface ObjectKeyToWrite : IsAutoId {
    fun theObject(): Any
    fun itsKey(): List<Any>
    fun itsHashCode(): Int
    fun keyToWrite(counter: Int?): List<Any>
    fun toObjectKey(hashCounter: Int? = null): ObjectKey
}

internal data class PersistenceKey(val keys: List<Any>) : ObjectKey {
    override fun keys() = keys
    override fun isAutoId() = false
    override fun hashCounter() = throw RuntimeException("no autoKey")
}

internal data class HashCodeKey(val keys: List<Any>, val hashCodeX: Int, val hashCodeCounter: Int) : ObjectKey {
    override fun isAutoId() = true

    override fun keys(): List<Any> = listOf(hashCodeX, hashCodeCounter)

    override fun hashCounter() = hashCodeCounter
}


