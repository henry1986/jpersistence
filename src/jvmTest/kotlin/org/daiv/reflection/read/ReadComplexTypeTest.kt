package org.daiv.reflection.read

import org.daiv.reflection.common.PersisterProviderImpl
import org.daiv.reflection.common.SimpleTypeProperty
import org.daiv.reflection.common.SimpleTypes
import org.daiv.reflection.common.including
import org.daiv.reflection.persister.Persister
import org.daiv.reflection.persister.ReadCache
import org.daiv.runTest
import kotlin.reflect.KClass
import kotlin.reflect.full.createType
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals


class ReadComplexTypeTest {
    data class SimpleObject(val y: Int)
    data class ComplexObject(val x: Int, val s: SimpleObject)

    private fun KClass<*>.toReadComplexType(p: PersisterProviderImpl) =
        ReadComplexType(SimpleTypeProperty(this.createType(), simpleName!!), including(), p, null)

    val persister = Persister("ReadComplexTypeTest.db")
    val cO = ComplexObject(5, SimpleObject(6))
    val clazz = ComplexObject::class
    private val p = PersisterProviderImpl(persister, null)
    private val r = clazz.toReadComplexType(p)
    private val cache = ReadCache()
    init {
        cache.tableHandlers(p)
    }

    @Test
    fun testTableHead() {
        val tableHead = r.toTableHead()
        assertEquals("ComplexObject_x Int NOT NULL", tableHead)
    }

    @Test
    fun testInsert(){
        val ret = r.insertObject(cO, cache)
        assertEquals(listOf(SimpleTypes.InsertObjectImpl("ComplexObject_x", "5")), ret)
    }

    @Test
    fun testKey(){
        val key = r.key()
        assertEquals("ComplexObject_x", key)
    }

    @Test
    fun read() = runTest{
        val table = persister.Table(clazz)
        table.persist()
        table.insert(listOf(cO))
        val read = table.read(5)
//        persister.write("CREATE TABLE IF NOT EXISTS `ComplexObject` (x Int NOT NULL, s_y Int NOT NULL, PRIMARY KEY(x));")
//        persister.write("CREATE TABLE IF NOT EXISTS `SimpleObject` (y Int NOT NULL, PRIMARY KEY(y));")
//        persister.write("INSERT INTO `SimpleObject` (y ) VALUES (6);")
//        persister.write("INSERT INTO `ComplexObject` (x, s_y ) VALUES (5, 6);")
//        val ret = r.getValue(cache, object:ReadValue{
//            override fun getObject(number: Int): Any? {
//                return when(number){
//                    1 -> 5
//                    2 -> 6
//                    else -> throw RuntimeException("should not be $number")
//                }
//            }
//
//            override fun getLong(number: Int): Long? {
//                throw RuntimeException("there shouldn't be any long for $number")
//            }
//        }, 1, PersistenceKey(listOf(5)))
//        assertEquals(NextSize(ReadAnswer(cO, true), 2), ret)
    }

    @AfterTest
    fun afterTest(){
        persister.delete()
    }
}