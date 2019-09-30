/*
 * Copyright (c) 2018. Martin Heinrich - All Rights Reserved
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 */

package org.daiv.reflection.database

import org.daiv.reflection.persister.Persister
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement

interface DatabaseInterface : SimpleDatabase {
    val statement: Statement
    fun commit()
    val path: String
}

/**
 * class that automatically opens a connection - to create a new connection,
 * a new instance must be created. Calling [open] does nothing
 */
class DatabaseHandler constructor(override val path: String) : DatabaseInterface {

    private val connection: Connection

    init {
        try {
            Class.forName("org.sqlite.JDBC")
        } catch (e: ClassNotFoundException) {
            throw RuntimeException(e)
        }
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:$path")
            if (!connection!!.isClosed) {
                println("...Connection established to $path")
            }
        } catch (e: SQLException) {
            throw RuntimeException(e)
        }

        Runtime.getRuntime()
                .addShutdownHook(object : Thread() {
                    override fun run() {
                        close()
                    }
                })
    }

    override val statement: Statement
        get() {
            try {
                if (connection == null) {
                    throw NullPointerException("Database connection not opened")
                }
                return connection!!.createStatement()
            } catch (e: SQLException) {
                throw RuntimeException(e)
            }

        }

    override fun close() {
        try {
            if (!connection!!.isClosed && connection != null) {
                connection!!.close()
                if (connection!!.isClosed)
                    println("Connection to DatabaseInterface closed")
            }
        } catch (e: SQLException) {
            throw RuntimeException(e)
        }

    }

    override fun open() {
    }

    override fun delete(): Boolean {
        return File(path).delete()
    }

    override fun commit() {
        try {
            connection!!.commit()
        } catch (e: SQLException) {
            throw RuntimeException(e)
        }

    }
}

fun persister(path: String) = Persister(DatabaseHandler(path))

