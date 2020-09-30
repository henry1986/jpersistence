package org.daiv.reflection.persistence.kotlin

import kotlinx.coroutines.*
import mu.KotlinLogging
import org.daiv.reflection.persister.InsertCachePreference
import org.daiv.reflection.persister.Persister
import kotlin.test.assertEquals

//class ParallelTest
//    : Spek({
//               data class InnerClass(val x: Int, val y: Int, val z: String)
//               data class OuterClass(val x: Int, val inner: InnerClass)
//               data class OuterClass2(val x: Int, val inner: InnerClass)
//               describe("") {
//                   val logger = KotlinLogging.logger {}
//                   val persister = Persister("ParallelTest.db")
//                   val tableOuter = persister.Table(OuterClass::class)
//                   tableOuter.persist()
//                   val tableOuter2 = persister.Table(OuterClass2::class)
//                   tableOuter2.persist()
//                   val o1 = OuterClass(5, InnerClass(1, 2, "Hello"))
//                   val o2 = OuterClass(6, InnerClass(3, 5, "World"))
//                   val o3 = OuterClass(7, InnerClass(6, 5, "World"))
//                   val o4 = OuterClass(8, InnerClass(9, 5, "World"))
//                   val list = listOf(o1, o2, o3, o4)
//                   val o21 = OuterClass2(5, InnerClass(1, 2, "Hello"))
//                   val o22 = OuterClass2(6, InnerClass(3, 5, "World"))
//                   val o23 = OuterClass2(7, InnerClass(6, 5, "World"))
//                   val o24 = OuterClass2(8, InnerClass(9, 5, "World"))
//                   val list2 = listOf(o21, o22, o23, o24)
//                   on("test parallelCache") {
//                       val n = newFixedThreadPoolContext(8, "hello")
//                       val cache = runBlocking {
//                           val mainScope = CoroutineScope(n)
//                           val cache = persister.ParallelCommonCache(mainScope, this, true, InsertCachePreference(true, false))
////                           val cache = persister.SeriellCommonCache(InsertCachePreference(true))
//                           val cacheOuter = cache.onTable(tableOuter)
//                           logger.trace { "start 1" }
//                           cacheOuter.insert(list)
//                           logger.trace { "started 1" }
//                           val cacheOuter2 = cache.onTable(tableOuter2)
//                           logger.trace { "start 2" }
//                           cacheOuter2.insert(list2)
//                           logger.trace { "start commit" }
//                           cache
//                       }
//                       cache.commit()
//                       it("test correctly written") {
//                           persister.clearCache()
//                           val read = tableOuter.readAll()
//                           assertEquals(list, read)
//                           persister.clearCache()
//                           val read2 = tableOuter2.readAll()
//                           assertEquals(list2, read2)
//                       }
//                   }
//                   afterGroup { persister.delete() }
//               }
//           })