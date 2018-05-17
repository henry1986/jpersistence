package org.daiv.reflection.persistence.kotlin

import org.daiv.immutable.utils.persistence.annotations.DatabaseWrapper
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on

class CompositeKeyTest : Spek({
    given(""){
        on(""){
            it(""){
                val d = DatabaseWrapper.create("CompositeTest.db")
                d.open()
                d.statement.execute("CREATE TABLE Composite (x Int NOT NULL, y Int NOT NULL, z Text NOT NULL, PRIMARY KEY(x,y))")
            }
        }
    }
})