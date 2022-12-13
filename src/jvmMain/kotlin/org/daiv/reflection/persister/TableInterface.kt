package org.daiv.reflection.persister

interface Clearable {
    fun clear()
}

interface Persistable{
    fun persist()
}

interface TableNameable{
    val tableName: String
}

interface TableInterface<T : Any> : Clearable, Persistable, TableNameable {
    fun readAll(): List<T>
    fun readRequest(request:String):List<T>
    fun read(fieldName: String, key: Any): List<T>
    fun insert(list: List<T>)
    fun insert(t: T)
    suspend fun suspendInsert(t: T)
    suspend fun suspendInsert(list: List<T>)
    fun delete(fieldName: String, key: Any)
    fun delete(id: Any)
    fun read(key: Any): T?
    fun exists(fieldName: String, key: Any):Boolean
    fun read(accessObject: AccessObject, p:List<PropertyValue<*>>): List<T>{
        return readRequest("select * from $tableName where ${p.toRequest(accessObject)}")
    }
}

class NoPersistantTable<T : Any>(
    override val tableName:String,
    val fieldGetter: T.(String) -> Any,
    val keyGetter: T.(Any) -> Boolean,
    val readRequestFunc:(String) -> List<T>,
    setFactory: () -> MutableSet<T> = { mutableSetOf() }
) : TableInterface<T> {
    val set = setFactory()
    override fun persist(){

    }

    override fun readRequest(request: String): List<T> {
        return this.readRequestFunc(request)
    }

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

    override suspend fun suspendInsert(list: List<T>) {
        this.set.addAll(list)
    }

    override suspend fun suspendInsert(t: T) {
        this.set.add(t)
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

    override fun exists(fieldName: String, key: Any): Boolean {
        return read(fieldName, key).isNotEmpty()
    }
}
