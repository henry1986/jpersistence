/*******************************************************************************
 * Copyright (c) 2018 Martin Heinrich
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 *
 * Contributors:
 *    Martin Heinrich - developer and maintainer
 *******************************************************************************/
package org.daiv.reflection.persistence;

import org.daiv.immutable.utils.persistence.annotations.FlatList;
import org.daiv.immutable.utils.persistence.annotations.ToPersistence;

import java.util.List;

public class NestedListObject {

	private final long i1;
	private final List<PersisterObject> ps;

	private NestedListObject(long i1, List<PersisterObject> ps) {
		super();
		this.i1 = i1;
		this.ps = ps;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (i1 ^ (i1 >>> 32));
		result = prime * result + ((ps == null) ? 0 : ps.hashCode());
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
		NestedListObject other = (NestedListObject) obj;
		if (i1 != other.i1)
			return false;
		if (ps == null) {
			if (other.ps != null)
				return false;
		} else if (!ps.equals(other.ps))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return " [" + i1 + ", " + ps + "]";
	}

	@ToPersistence(elements = {
			@FlatList(name = "i1"),
			@FlatList(name = "ps", size = 2),
	})
	public static NestedListObject create(long i1, List<PersisterObject> ps) {
		return new NestedListObject(i1, ps);
	}
}
