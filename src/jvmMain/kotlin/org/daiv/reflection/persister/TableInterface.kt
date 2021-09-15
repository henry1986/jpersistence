package org.daiv.reflection.persister

interface Clearable {
    fun clear()
}

interface TableInterface<T : Any> : Clearable {
    fun readAll(): List<T>
    fun read(fieldName: String, key: Any): List<T>
    fun insert(list: List<T>)
    fun insert(t: T)
    fun delete(fieldName: String, key: Any)
    fun delete(id: Any)
    fun read(key: Any): T?
}

class NoPersistantTable<T : Any>(
    val fieldGetter: T.(String) -> Any,
    val keyGetter: T.(Any) -> Boolean,
    setFactory: () -> MutableSet<T> = { mutableSetOf() }
) : TableInterface<T> {
    val set = setFactory()
    override fun clear() {
        set.clear()
    }

    override fun read(key: Any): T? {
        return set.find { it.keyGetter(key) }
    }

    override fun readAll(): List<T> {
        return set.toList()
    }

    override fun read(fieldName: String, key: Any): List<T> {
        return set.filter { it.fieldGetter(fieldName) == key }
    }

    override fun insert(list: List<T>) {
        this.set.addAll(list)
    }

    override fun insert(t: T) {
        this.set.add(t)
    }

    override fun delete(id: Any) {
        set.find { it.keyGetter(id) }?.let { data ->
            set.remove(data)
        }
    }

    override fun delete(fieldName: String, key: Any) {
        val read = read(fieldName, key)
        set.removeAll(read)
    }
}
