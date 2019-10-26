package org.daiv.reflection.persister

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.launch
import mu.KotlinLogging
import org.daiv.reflection.read.ReadPersisterData

internal interface ActorHandlerInterface {
    val map: Map<InsertKey, () -> InsertRequest>
    suspend fun launch(block: suspend () -> Unit)

}

internal interface TableHandler : ActorHandlerInterface {
    suspend fun send(requestTask: RequestTask)
}

internal class SequentialTableHandler(val readTableCache: ReadTableCache) : TableHandler {
    override val map: MutableMap<InsertKey, () -> InsertRequest> = mutableMapOf()

    override suspend fun send(requestTask: RequestTask) {
        if (!map.containsKey(requestTask.insertKey)) {
            requestTask.put(map, readTableCache)
            requestTask.toDo()
        } else{
//            println("insereted")
        }
    }

    override suspend fun launch(block: suspend () -> Unit) {
        block()
    }
}

private data class ActorResponse(val requestTask: RequestTask, val completableDeferred: CompletableDeferred<Boolean>)


internal class ActorHandler(val actorScope: CoroutineScope,
                            private val requestScope: CoroutineScope,
                            private val readTableCache: ReadTableCache) : ActorHandlerInterface {
    val logger = KotlinLogging.logger {}
//    private val existingSet: MutableSet<List<Any>> = mutableSetOf()
    override val map: Map<InsertKey, () -> InsertRequest>
        get() = internalMap

    private val internalMap: MutableMap<InsertKey, () -> InsertRequest> = mutableMapOf()

    fun check(requestTask: RequestTask) {
        if (!map.containsKey(requestTask.insertKey)) {
//            existingSet.add(requestTask.insertKey.key)
            requestTask.put(internalMap, readTableCache)

//            requestTask.toDo()
            requestScope.launch { requestTask.toDo() }
        } else {
//            logger.info { "already insert" }
        }
    }

    override suspend fun launch(block: suspend () -> Unit) {
        block()
    }
}

internal class InsertCompletableActor(private val actorHandler: ActorHandler) :
        TableHandler, ActorHandlerInterface by actorHandler {

    private val actor = actorHandler.actorScope.actor<ActorResponse> {
        for (requestTask in channel) {
            actorHandler.check(requestTask.requestTask)
            requestTask.completableDeferred.complete(true)
        }
    }

    override suspend fun send(requestTask: RequestTask) {
        val c = CompletableDeferred<Boolean>()
        actor.send(ActorResponse(requestTask, c))
        c.await()
    }
}

internal class InsertActor(private val actorHandler: ActorHandler) :
        TableHandler, ActorHandlerInterface by actorHandler {
    private val actor = actorHandler.actorScope.actor<RequestTask> {
        for (requestTask in channel) {
            actorHandler.check(requestTask)
        }
    }

    override suspend fun send(requestTask: RequestTask) {
        actor.send(requestTask)
    }
}

internal class RequestTask(val insertKey: InsertKey,
                           val obj: Any? = null,
                           val toBuild: () -> InsertRequest,
                           val toDo: suspend () -> Unit) {
    constructor(insertKey: InsertKey, toBuild: () -> InsertRequest, toDo: suspend () -> Unit) :
            this(insertKey, null, toBuild, toDo)

    fun put(map: MutableMap<InsertKey, () -> InsertRequest>, readTableCache: ReadTableCache) {
        map[insertKey] = toBuild
        obj?.let {
            readTableCache[insertKey] = it
        }
    }
}

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
