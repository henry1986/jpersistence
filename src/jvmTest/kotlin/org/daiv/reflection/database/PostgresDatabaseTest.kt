package org.daiv.reflection.database

import mu.KotlinLogging
import org.daiv.reflection.persister.Persister
import org.daiv.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

data class TestConnectionData(val connectionData: PostgresConnectionData, val testName: String) {
    fun buildName(dbName: String) = "${testName}_$dbName"
    fun persister(dbName: String) = connectionData.persister(buildName(dbName))
    fun create(dbName: String) = connectionData.create(buildName(dbName))
    fun default() = connectionData.default()
}

class PostgresDatabaseTest {
    val logger = KotlinLogging.logger {}

    data class FirstDataTest(val x: Int, val y: Int)

    val connectionData =
        TestConnectionData(PostgresConnectionData("localhost", 5432, "pguser", "12345"), "PostgresDatabaseTest")

    // to test, it needs a postgres db running
//    @Test
    fun startDb() = runTest{
        val p = connectionData.persister("startDB")
        try {
            val table = p.Table(FirstDataTest::class)
            table.persist()
            val list = listOf(FirstDataTest(5, 6))
            table.insert(list)
            val readAll = table.readAll()
            assertEquals(list, readAll)
            val read= table.read(5)
            assertEquals(list.first(), read)
        } finally {
            p.delete()
        }
    }

//    @Test
    fun checkDBCreationAndDeletion() {
        val testDBName = "checkDBCreationAndDeletionDB"
        val buildName = connectionData.buildName(testDBName)
        val created = connectionData.create(testDBName)
        val def = connectionData.default()
        assertTrue(def.listDBs().any { it == buildName })
        created.delete()
        assertTrue(def.listDBs().none { it == buildName })
        def.close()
    }

}