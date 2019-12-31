package org.daiv.reflection.persister

import org.daiv.reflection.read.ReadPersisterData


class InsertCacheHandler<R : Any> internal constructor(internal val map: InsertMap,
                                                       internal val readPersisterData: ReadPersisterData,
                                                       val table: Persister.Table<R>,
                                                       override val isParallel: Boolean) : InsertCache<R> {
    override suspend fun insert(o: List<R>) {
        if (o.isEmpty()) {
            return
        }
        map.actors[table]!!.launch {
            readPersisterData.putInsertRequests(map, o, table)
        }
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
