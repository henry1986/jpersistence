package org.daiv.reflection.database

interface SimpleDatabase {
    /**
     * closes the database connection
     */
    fun close()

    /**
     * opens the database connection
     */
    fun open()

    /**
     * deletes the database file completely
     */
    fun delete(): Boolean
}