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


import org.daiv.immutable.utils.persistence.annotations.DatabaseInterface;
import org.daiv.immutable.utils.persistence.annotations.DatabaseWrapper;

public class PersisterTest {

//	@Test
//	public void test1() {
//		DatabaseInterface databaseInterface = DatabaseWrapper.create(ImUtil.getRoot() + "PersisterTest1.db");
//		try {
//			databaseInterface.open();
//			Persister create = Persister.create(databaseInterface);
//			create.persist(ComplexObject.class);
//			ComplexObject c = ComplexObject.create(5, 18, PersisterObject.create(9, 3d, 6, "hallo"));
//			create.insert(c);
//			ComplexObject read = create.read(ComplexObject.class, 5);
//			assertEquals(c, read);
//			System.out.println(read);
//			databaseInterface.close();
//		} catch (Throwable t) {
//			throw t;
//		} finally {
//			databaseInterface.delete();
//		}
//	}
//
//	@Test
//	public void testList() {
//		NestedListObject n = NestedListObject.create(5,
//				ImList.create(PersisterObject.create(4, 1d, 6, "hallo2"),
//						PersisterObject.create(8, 2d, 6, "hallo3")));
//
//		DatabaseInterface databaseInterface = DatabaseWrapper.create(ImUtil.getRoot() + "PersisterListTest.db");
//		try {
//			databaseInterface.open();
//			Persister create = Persister.create(databaseInterface);
//			create.persist(NestedListObject.class);
//			create.insert(n);
//			NestedListObject read = create.read(NestedListObject.class, 5);
//			System.out.println(read);
//			assertEquals(n, read);
//			databaseInterface.close();
//		} catch (Throwable t) {
//			throw t;
//		} finally {
//			databaseInterface.delete();
//		}
//	}

}
