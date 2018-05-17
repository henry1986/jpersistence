package org.daiv.reflection.read

import org.daiv.reflection.common.FieldData
import java.sql.ResultSet

interface ReadFieldData<T : Any> :FieldData<T>{

    /**
     * this method creates the string for the sql command "CREATE TABLE".
     *
     * @param prefix
     * a possible prefix for the variables name. Null, if no prefix
     * is wanted.
     * @return the string for the "CREATE TABLE" command
     */
    fun toTableHead(prefix: String?, key: Boolean): String

    fun getValue(resultSet: ResultSet, number: Int): T

}