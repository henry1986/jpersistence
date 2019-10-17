package org.daiv.reflection.persister

/**
 * [useCache] activates the use of the cache per [Persister] instance. Otherwise a readCache is only activated
 * per Request
 * if [useCache] is true, after [clearCacheAfterNumberOfStoredObjects] the cache is cleared to prevent memory leak
 */
data class PersisterPreference(val useCache: Boolean, val clearCacheAfterNumberOfStoredObjects: Int)

fun defaultPersisterPreference() = PersisterPreference(false, 1000000)