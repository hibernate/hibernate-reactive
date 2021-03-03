/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import org.hibernate.dialect.CockroachDB192Dialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.PostgreSQL9Dialect;

/**
 * PostgreSQL has a "funny" parameter syntax of form {@code $n}, which
 * the Vert.x {@link io.vertx.sqlclient.SqlClient} does not abstract.
 * This class converts JDBC/ODBC-style {@code ?} parameters generated
 * by Hibernate ORM to this native format.
 */
public class Parameters {

	private static Parameters INSTANCE = new Parameters();

	private static Parameters NO_PARSING = new Parameters() {
		@Override
		public String process(String sql) {
			return sql;
		}

		@Override
		public String process(String sql, int parameterCount) {
			return sql;
		}

		@Override
		public String processLimit(String sql, Object[] parameterArray, boolean hasOffset) {
			return sql;
		}
	};

	public static Parameters instance(Dialect dialect) {
		return dialect instanceof PostgreSQL9Dialect || dialect instanceof CockroachDB192Dialect
				? INSTANCE
				: NO_PARSING;
	}

	private Parameters() {
	}

	public String process(String sql) {
		if ( isProcessingNotRequired( sql ) ) {
			return sql;
		}
		return new Parser( sql ).result();
	}

	/**
	 * Limit and offset gets applied just before the execution of the query but because we know
	 * how the string looks like for Postgres, it's faster to replace the last bit instead
	 * of processing the whole query
	 */
	public String processLimit(String sql, Object[] parameterArray, boolean hasOffset) {
		if ( isProcessingNotRequired( sql ) ) {
			return sql;
		}

		// Replace 'limit ? offset ?' with the $ style parameters for PostgreSQL
		int index = hasOffset ? parameterArray.length - 1 : parameterArray.length;
		int pos = sql.indexOf( " limit ?" );
		if ( pos > -1 ) {
			String sqlProcessed = sql.substring( 0, pos ) + " limit $" + index++;
			if ( hasOffset ) {
				sqlProcessed += " offset $" + index;
			}
			return sqlProcessed;
		}

		return sql;
	}

	/**
	 * Replace all JDBC-style {@code ?} parameters with Postgres-style
	 * {@code $n} parameters in the given SQL string.
	 */
	public String process(String sql, int parameterCount) {
		if ( isProcessingNotRequired( sql ) ) {
			return sql;
		}
		return new Parser( sql, parameterCount ).result();
	}

	private static boolean isProcessingNotRequired(String sql) {
		return sql == null
				// There aren't any parameters
				|| sql.indexOf( '?' ) == -1;
	}

	private static class Parser {

		private boolean inString;
		private boolean inQuoted;
		private boolean inSqlComment;
		private boolean inCComment;
		private boolean escaped;
		private int count = 0;
		private StringBuilder result;
		private int previous;

		private Parser(String sql) {
			this( sql, 10 );
		}

		private Parser(String sql, int parameterCount) {
			result = new StringBuilder( sql.length() + parameterCount );
			sql.codePoints().forEach( this::append );
		}

		private String result() {
			return result.toString();
		}

		private void append(int codePoint) {
			if ( escaped ) {
				escaped = false;
			}
			else {
				switch ( codePoint ) {
					case '\\':
						escaped = true;
						break;
					case '"':
						if ( !inString && !inSqlComment && !inCComment ) inQuoted = !inQuoted;
						break;
					case '\'':
						if ( !inQuoted && !inSqlComment && !inCComment ) inString = !inString;
						break;
					case '-':
						if ( !inQuoted && !inString && !inCComment && previous == '-' ) inSqlComment = true;
						break;
					case '\n':
						inSqlComment = false;
						break;
					case '*':
						if ( !inQuoted && !inString && !inSqlComment && previous == '/' ) inCComment = true;
						break;
					case '/':
						if ( previous == '*' ) inCComment = false;
						break;
					//TODO: $$-quoted strings
					case '?':
						if ( !inQuoted && !inString ) {
							result.append( '$' ).append( ++count );
							previous = '?';
							return;
						}
				}
			}
			previous = codePoint;
			result.appendCodePoint( codePoint );
		}
	}
}
