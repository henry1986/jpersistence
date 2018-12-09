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

package org.daiv.reflection.annotations

@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.RUNTIME)
annotation class ManyToOne(val tableName: String = "")

@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.RUNTIME)
annotation class ManyList(val tableName: String = "")

@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.RUNTIME)
annotation class ManyMap(val tableNameKey: String = "", val tableNameValue: String = "")


@Target(AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.RUNTIME)
annotation class SameTable


data class TableData(val tableName: String, val header: List<String>, val values: List<List<String>>) {
    fun insertCmd() = "INSERT INTO $tableName ${header.joinToString(", ", "(", ")")} VALUES" +
            " ${values.joinToString(", ", postfix = ";") { it.joinToString(", ", "(", ")") }};"

    fun insertCmd(index: Int) = "INSERT INTO $tableName ${header.joinToString(", ", "(", ")")} VALUES" +
            " ${values[index].joinToString(", ", "(", ")")};"

    fun deleteCmd(index: Int) = "DELETE FROM $tableName WHERE ${header.first()} = ${values[index].first()};"

    fun existsCmd(index: Int) =
        "SELECT EXISTS( SELECT 1 FROM $tableName WHERE ${header.first()} = ${values[index].first()} LIMIT 1);"

    fun canAppend(list: List<String>): Boolean {
        val key = list.first()
        return values.none { it.first() == key }
    }

    fun appendValues(list: List<String>) = copy(values = values.plus(element = list))
}

data class AllTables(val tableData: TableData, val helper: List<TableData>, val keyTables: List<TableData>){
    val all = helper + keyTables


}