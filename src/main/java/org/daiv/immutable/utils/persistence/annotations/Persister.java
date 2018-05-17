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
package org.daiv.immutable.utils.persistence.annotations;

import org.daiv.reflection.read.ReadPersisterData;
import org.daiv.reflection.write.WritePersisterData;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Persister {

	private final Statement statement;

	private Persister(Statement statement) {
		super();
		this.statement = statement;
	}

	private <T> String createTable(Class<T> clazz) {
		return ReadPersisterData.Companion.create(clazz)
							.createTable();
	}

	private ResultSet read(String query) {
		try {
			return statement.executeQuery(query);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private void write(String query) {
		try {
			statement.execute(query);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public <T> void persist(Class<T> clazz) {
		String createTable = createTable(clazz);
		System.out.println("table: "+createTable);
		write(createTable);
	}

	public void insert(Object o) {
		String createTable = WritePersisterData.Companion.create(o)
														 .insert();
		System.out.println(createTable);
		write(createTable);
	}

	public <T> T read(Class<T> clazz, Object id) {
		ReadPersisterData<T> persisterData = ReadPersisterData.Companion.create(clazz);
		String query = "SELECT * FROM " + persisterData
				.getTableName() + " WHERE " + persisterData.getIdName() + " = " + id.toString() + ";";
		System.out.println(query);
		ResultSet execute = read(query);
		return persisterData.read(execute);
	}

	public static Persister create(DatabaseInterface databaseInterface) {
		return new Persister(databaseInterface.getStatement());
	}
}
