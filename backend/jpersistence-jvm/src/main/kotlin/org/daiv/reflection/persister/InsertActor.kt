package org.daiv.reflection.persister

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.launch
import mu.KotlinLogging
import org.daiv.reflection.plain.ObjectKey

internal interface ActorHandlerInterface {
    val map: Map<InsertKey, () -> List<InsertRequest>>
    suspend fun launch(block: suspend () -> Unit)

}

internal interface TableHandler : ActorHandlerInterface {
    suspend fun send(requestTask: RequestTask)
}

internal class SequentialTableHandler(val readTableCache: ReadTableCache) : TableHandler {
    override val map: MutableMap<InsertKey, () -> List<InsertRequest>> = mutableMapOf()

    override suspend fun send(requestTask: RequestTask) {
        if (requestTask.put(map, readTableCache)) {
            requestTask.toDo()
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
    override val map: Map<InsertKey, () -> List<InsertRequest>>
        get() = internalMap

    private val internalMap: MutableMap<InsertKey, () -> List<InsertRequest>> = mutableMapOf()

    fun check(requestTask: RequestTask) {
        if (requestTask.put(internalMap, readTableCache)) {
            requestScope.launch { requestTask.toDo() }
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

//internal interface RequestTask {
//    fun put(map: MutableMap<InsertKey, () -> InsertRequest>, readTableCache: ReadTableCache): Boolean
//    val toDo: suspend () -> Unit
//}

internal interface RequestTask {
    val insertKey: InsertKey
    val toDo: suspend () -> Unit
    fun put(map: MutableMap<InsertKey, () -> List<InsertRequest>>, readTableCache: ReadTableCache): Boolean
}

internal class HelperRequestTask(override val insertKey: InsertKey,
                                 val toBuild: () -> List<InsertRequest>,
                                 override val toDo: suspend () -> Unit) : RequestTask {
    override fun put(map: MutableMap<InsertKey, () -> List<InsertRequest>>, readTableCache: ReadTableCache): Boolean {
        if (!map.containsKey(insertKey)) {
            map[insertKey] = toBuild
            return true
        }
        return false
    }
}

internal class DefaultRequestTask constructor(override val insertKey: InsertKey,
                                              val obj: Any,
                                              val toBuild: (ObjectKey) -> List<InsertRequest>,
                                              override val toDo: suspend () -> Unit) : RequestTask {

    override fun put(map: MutableMap<InsertKey, () -> List<InsertRequest>>, readTableCache: ReadTableCache): Boolean {
        if (!map.containsKey(insertKey)) {
            val readKey = readTableCache.createReadKey(insertKey.key)
            map[insertKey] = { toBuild(readKey.key) }
            readTableCache[readKey] = obj
            return true
        }
        return false
    }

}

//internal class ListRequestTask(val insertKey: InsertKey,
//                               val referenceObjectKey: List<Any>,
//                               val obj: Any? = null,
//                               val toBuild: () -> InsertRequest,
//                               override val toDo: suspend () -> Unit) : RequestTask {
//
//    override fun put(map: MutableMap<InsertKey, () -> InsertRequest>, readTableCache: ReadTableCache): Boolean {
//        if (!map.containsKey(insertKey)) {
//            map[insertKey] = toBuild
//            obj?.let {
//                readTableCache[insertKey] = it
//            }
//            return true
//        }
//        return false
//    }
//
//}
