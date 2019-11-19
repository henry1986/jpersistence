package org.daiv.reflection.persister

import org.daiv.reflection.read.ReadPersisterData


class InsertCacheHandler<R : Any> internal constructor(internal val map: InsertMap,
                                                       internal val readPersisterData: ReadPersisterData<R, Any>,
                                                       val table: Persister.Table<R>,
                                                       override val isParallel: Boolean) : InsertCache<R> {
    override suspend fun insert(o: List<R>) {
        if (o.isEmpty()) {
            return
        }
        readPersisterData.putInsertRequests(table._tableName, map, o)
    }

    override fun commit() {
        map.insertAll()
        table.tableEvent()
    }
}


interface InsertCache<R : Any> {
    val isParallel: Boolean
    suspend fun insert(o: List<R>)
    fun commit()
}