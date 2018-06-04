package org.daiv.reflection.persistence.kotlin

import org.daiv.immutable.utils.persistence.annotations.DatabaseWrapper
import org.daiv.reflection.persistence.ComplexObject
import org.daiv.reflection.persistence.PersisterObject
import org.daiv.reflection.persister.Persister
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import kotlin.test.assertEquals


class PersisterTest :
    Spek({
             data class SimpleObject(val x: Int, val y: String)
             data class ComplexKObject(val x: Int, val y: SimpleObject)
             data class ComplexKey(val x: SimpleObject, val string: String)
             data class ReadValue(val id: Int, val string: String)

             val simpleObject = SimpleObject(5, "Hallo")

             val s = ObjectToTest(simpleObject,
                                  "(x Int NOT NULL, y Text NOT NULL, PRIMARY KEY(x))",
                                  "(x, y ) VALUES (5, \"Hallo\")", "KotlinSimpleObject", 5)
             val persisterObject = PersisterObject(5, 3.0, 1, "Hallo")
             val p = ObjectToTest(persisterObject,
                                  "(i1 Int NOT NULL, d2 Double NOT NULL, i2 Int NOT NULL, s1 Text NOT NULL, PRIMARY KEY(i1))",
                                  "(i1, d2, i2, s1 ) VALUES (5, 3.0, 1, \"Hallo\")",
                                  "JavaSimpleObject", 5)
             val c = ObjectToTest(ComplexObject(3, 6, persisterObject),
                                  "(i1 Int NOT NULL, i3 Int NOT NULL, p1_i1 Int NOT NULL, p1_d2 Double NOT NULL, "
                                          + "p1_i2 Int NOT NULL, p1_s1 Text NOT NULL, PRIMARY KEY(i1))",
                                  "(i1, i3, p1_i1, p1_d2, p1_i2, p1_s1 ) VALUES (3, 6, 5, 3.0, 1, \"Hallo\")",
                                  "JavaComplexObject",
                                  3)
             val k = ObjectToTest(ComplexKObject(1, simpleObject),
                                  "(x Int NOT NULL, y_x Int NOT NULL, y_y Text NOT NULL, PRIMARY KEY(x))",
                                  "(x, y_x, y_y ) VALUES (1, 5, \"Hallo\")",
                                  "KotlinComplexObject",
                                  1)

             val key = ComplexKey(simpleObject, "hello")
             val t = ObjectToTest(key,
                                  "(x_x Int NOT NULL, x_y Text NOT NULL, string Text NOT NULL, PRIMARY KEY(x_x, x_y))",
                                  "(x_x, x_y, string ) VALUES (5, \"Hallo\", \"hello\")",
                                  "KotlinComplexKeyObject",
                                  simpleObject)

             val listOf = listOf(s, p, c, k, t)
             val database = DatabaseWrapper.create("ReadValue.db")


             describe("Persister Table creation, read and insert") {
                 listOf.forEach { o ->
                     o.open()
                     on("persist $o") {
                         it("check createTable") {
                             o.checkCreateTable()
                         }
                         it("check insert") {
                             o.checkInsert()
                         }
                         o.beforeReadFromDatabase()
                         it("check readData") {
                             o.checkReadData()
                         }
                     }
                     on("read not from key") {
                         database.open()

                         val readValue1 = ReadValue(5, "HalloX")
                         val readValue2 = ReadValue(6, "HollaY")
                         val persister = Persister(database)
                         persister.persist(ReadValue::class)
                         persister.insert(readValue1)
                         persister.insert(readValue2)
                         it("read from column") {
                             val read = persister.read(ReadValue::class, "string", "HalloX")
                             assertEquals(readValue1, read)
                         }
                     }
                     afterGroup { o.delete(); database.delete() }
                 }
             }
         })

