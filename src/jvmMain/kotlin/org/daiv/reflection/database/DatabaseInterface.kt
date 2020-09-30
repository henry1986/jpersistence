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

import mu.KotlinLogging
import org.daiv.reflection.persister.*
import org.daiv.reflection.read.Mapper
import org.slf4j.MarkerFactory
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

    val logger = KotlinLogging.logger {}

    private val connection: Connection

    init {
        try {
            Class.forName("org.sqlite.JDBC")
        } catch (e: ClassNotFoundException) {
            throw e
        }
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:$path")
            if (!connection!!.isClosed) {
                logger.info(dbOpen, "...Connection established to $path")
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
                    throw NullPointerException("Database connection to $path not opened")
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
                    logger.info(dbClose, "Connection to database $path closed")
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

fun persister(path: String, persisterPreference: PersisterPreference = defaultPersisterPreference()) =
        Persister(DatabaseHandler(path), emptyList(), persisterPreference)

fun mappedPersister(path: String, mapper: List<Mapper<*, *>>) = Persister(path, mapper)
