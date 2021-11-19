/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

public class OracleParameters extends Parameters {

	public static final OracleParameters INSTANCE = new OracleParameters();

	private OracleParameters() {
	}

	public String process(String sql) {
		if ( isProcessingNotRequired( sql ) ) {
			return sql;
		}
		String result = new OracleParameters.Parser( sql ).result();
		return result;
	}

	/**
	 * Limit and offset gets applied just before the execution of the query but because we know
	 * how the string looks like for Oracle, it's faster to replace the last bit instead
	 * of processing the whole query
	 */
	public String processLimit(String sql, Object[] parameterArray, boolean hasOffset) {
		if ( isProcessingNotRequired( sql ) ) {
			return sql;
		}

		throw new UnsupportedOperationException();
	}

	/**
	 * Replace all JDBC-style {@code ?} parameters with Oracle-style
	 * {@code :n} parameters in the given SQL string.
	 */
	public String process(String sql, int parameterCount) {
		if ( isProcessingNotRequired( sql ) ) {
			return sql;
		}
		return new Parser( sql, parameterCount ).result();
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
						if ( !inString && !inSqlComment && !inCComment ) {
							inQuoted = !inQuoted;
						}
						break;
					case '\'':
						if ( !inQuoted && !inSqlComment && !inCComment ) {
							inString = !inString;
						}
						break;
					case '-':
						if ( !inQuoted && !inString && !inCComment && previous == '-' ) {
							inSqlComment = true;
						}
						break;
					case '\n':
						inSqlComment = false;
						break;
					case '*':
						if ( !inQuoted && !inString && !inSqlComment && previous == '/' ) {
							inCComment = true;
						}
						break;
					case '/':
						if ( previous == '*' ) {
							inCComment = false;
						}
						break;
					//TODO: $$-quoted strings
					case '?':
						if ( !inQuoted && !inString ) {
							result.append( ':' ).append( ++count );
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
