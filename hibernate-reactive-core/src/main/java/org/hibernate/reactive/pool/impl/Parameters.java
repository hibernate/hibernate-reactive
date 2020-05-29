/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

/**
 * PostgreSQL has a "funny" parameter syntax of form {@code $n}, which
 * the Vert.x {@link io.vertx.sqlclient.SqlClient} does not abstract.
 * This class converts JDBC/ODBC-style {@code ?} parameters generated
 * by Hibernate ORM to this native format.
 */
public class Parameters {

	private boolean inString;
	private boolean inQuoted;
	private boolean inSqlComment;
	private boolean inCComment;
	private boolean escaped;
	private int count = 0;
	private StringBuilder result;
	private int previous;

	private Parameters(String sql, int parameterCount) {
		result = new StringBuilder( sql.length() + parameterCount );
		sql.codePoints().forEach(this::append);
	}

	private String result() {
		return result.toString();
	}

	private void append(int codePoint) {
		if (escaped) {
			escaped = false;
		}
		else {
			switch (codePoint) {
				case '\\':
					escaped = true;
					break;
				case '"':
					if (!inString && !inSqlComment && !inCComment) inQuoted = !inQuoted;
					break;
				case '\'':
					if (!inQuoted && !inSqlComment && !inCComment) inString = !inString;
					break;
				case '-':
					if (!inQuoted && !inString && !inCComment && previous=='-') inSqlComment = true;
					break;
				case '\n':
					inSqlComment = false;
					break;
				case '*':
					if (!inQuoted && !inString && !inSqlComment && previous=='/') inCComment = true;
					break;
				case '/':
					if (previous=='*') inCComment = false;
					break;
				//TODO: $$-quoted strings
				case '?':
					if (!inQuoted && !inString) {
						result.append('$').append(++count);
						previous = '?';
						return;
					}
			}
		}
		previous = codePoint;
		result.appendCodePoint(codePoint);
	}

	/**
	 * Replace all JDBC-style {@code ?} parameters with Postgres-style
	 * {@code $n} parameters in the given SQL string.
	 */
	public static String process(String sql, int parameterCount) {
		return new Parameters(sql, parameterCount).result();
	}

}
