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

package org.daiv.reflection.read

internal data class NextSize<T>(val t: T, val i: Int) {
    fun <R> transform(transformer: (T) -> R): NextSize<R> {
        return NextSize(transformer(t), i)
    }
}

internal interface InsertObject {
    /**
     * this method creates the string for the sql command "INSERT INTO ", but
     * only the values until "VALUES". For the values afterwards, see
     * [insertValue]
     *
     * @param prefix
     * a possible prefix for the variables name. Null, if no prefix
     * is wanted.
     *
     * @return the string for the "INSERT INTO " command
     */
    fun insertHead(): String

    /**
     * this method creates the string for the sql command "INSERT INTO ", but
     * only the values after "VALUES". For the values before, see
     * [insertHead]
     *
     * @return the string for the "INSERT INTO " command
     */
    fun insertValue(): String
}

internal fun List<InsertObject>.insertHeadString(): String {
    return asSequence().joinToString(", ") { it.insertHead() }
}

internal fun List<InsertObject>.insertValueString(): String {
    return asSequence().joinToString(", ") { it.insertValue() }
}

