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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseWrapper implements DatabaseInterface {

	private final String DB_PATH;

	private Connection connection;

	private DatabaseWrapper(String dB_PATH) {
		super();
		DB_PATH = dB_PATH;
	}

	public Connection getConnection() {
		return connection;
	}

	public void close() {
		try {

			if (!connection.isClosed() && connection != null) {
				connection.close();
				if (connection.isClosed())
					System.out.println("Connection to Database closed");
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public void open() {
		try {
			connection = DriverManager.getConnection("jdbc:sqlite:" + DB_PATH);
			if (!connection.isClosed()) {
				System.out.println("...Connection established to " + DB_PATH);
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		Runtime.getRuntime()
				.addShutdownHook(new Thread()
		{
					public void run() {
						close();
					}
				});
	}

	public boolean delete() {
		return new File(DB_PATH).delete();
	}

	public static DatabaseWrapper create(String path) {
		try {
			Class.forName("org.sqlite.JDBC");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		return new DatabaseWrapper(path);
	}

	@Override
	public Statement getStatement() {
		try {
			return connection.createStatement();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
