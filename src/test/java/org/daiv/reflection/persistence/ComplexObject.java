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

;

@PersistenceRoot(isJava = true)
public class ComplexObject {

	private final int i1;
	private final int i3;
	private final PersisterObject p1;

	@ToPersistence(elements = {
			@FlatList(name = "i1"),
			@FlatList(name = "i3"),
			@FlatList(name = "p1"),
	})
	public ComplexObject(int i1, int i3, PersisterObject p1) {
		super();
		this.i1 = i1;
		this.i3 = i3;
		this.p1 = p1;
	}

	@Override
	public String toString() {
		return "ComplexObject [i1=" + i1 + ", i3=" + i3 + ", p1=" + p1 + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + i1;
		result = prime * result + i3;
		result = prime * result + ((p1 == null) ? 0 : p1.hashCode());
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
		ComplexObject other = (ComplexObject) obj;
		if (i1 != other.i1)
			return false;
		if (i3 != other.i3)
			return false;
		if (p1 == null) {
			if (other.p1 != null)
				return false;
		} else if (!p1.equals(other.p1))
			return false;
		return true;
	}

	public static ComplexObject create(int i1, int i3, PersisterObject p1) {
		return new ComplexObject(i1, i3, p1);
	}
}
