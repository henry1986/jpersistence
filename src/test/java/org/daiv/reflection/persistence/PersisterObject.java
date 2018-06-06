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
package org.daiv.reflection.persistence;

import org.daiv.immutable.utils.persistence.annotations.FlatList;
import org.daiv.immutable.utils.persistence.annotations.PersistenceRoot;
import org.daiv.immutable.utils.persistence.annotations.ToPersistence;

@PersistenceRoot(isJava = true)
public class PersisterObject {

    private final int i1;
    private final double d2;
    private final int i2;
    private final String s1;

    @ToPersistence(elements = {
            @FlatList(name = "i1"),
            @FlatList(name = "d2"),
            @FlatList(name = "i2"),
            @FlatList(name = "s1"),
    })
    public PersisterObject(int i1, double d2, int i2, String s1) {
        super();
        this.i1 = i1;
        this.d2 = d2;
        this.i2 = i2;
        this.s1 = s1;
    }

    public int getI1() {
        return i1;
    }

    public double getD2() {
        return d2;
    }

    public int getI2() {
        return i2;
    }

    public String getS1() {
        return s1;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        long temp;
        temp = Double.doubleToLongBits(d2);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        result = prime * result + i1;
        result = prime * result + i2;
        result = prime * result + ((s1 == null) ? 0 : s1.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PersisterObject other = (PersisterObject) obj;
        if (Double.doubleToLongBits(d2) != Double.doubleToLongBits(other.d2))
            return false;
        if (i1 != other.i1)
            return false;
        if (i2 != other.i2)
            return false;
        if (s1 == null) {
            if (other.s1 != null)
                return false;
        } else if (!s1.equals(other.s1))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "[" + i1 + ", " + d2 + ", " + i2 + ", " + s1 + "]";
    }

    public static PersisterObject create(int i1, double d2, int i2, String s1) {
        return new PersisterObject(i1, d2, i2, s1);
    }
}
