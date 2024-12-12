/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import java.util.function.IntConsumer;

import org.hibernate.dialect.CockroachDialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.dialect.SQLServerDialect;

/**
 * Some databases have a different parameter syntax, which
 * the Vert.x {@link io.vertx.sqlclient.SqlClient} does not abstract.
 * This class converts JDBC/ODBC-style {@code ?} parameters generated
 * by Hibernate ORM to the native format.
 */
public abstract class Parameters {

	private final String paramPrefix;

	private static final Parameters NO_PARSING = new Parameters( null ) {
		@Override
		public String process(String sql) {
			return sql;
		}

		@Override
		public String process(String sql, int parameterCount) {
			return sql;
		}
	};

	protected Parameters(String paramPrefix) {
		this.paramPrefix = paramPrefix;
	}

	public static Parameters instance(Dialect dialect) {
		if ( dialect instanceof PostgreSQLDialect || dialect instanceof CockroachDialect ) {
			return PostgresParameters.INSTANCE;
		}
		if ( dialect instanceof SQLServerDialect ) {
			return SQLServerParameters.INSTANCE;
		}
		return NO_PARSING;
	}

	public static boolean isProcessingNotRequired(String sql) {
		return sql == null
				// There aren't any parameters
				|| sql.indexOf( '?' ) == -1;
	}

	public String process(String sql) {
		if ( isProcessingNotRequired( sql ) ) {
			return sql;
		}
		return new Parser( sql, paramPrefix ).result();
	}

	/**
	 * Replace all JDBC-style {@code ?} parameters with Postgres-style
	 * {@code $n} parameters in the given SQL string.
	 */
	public String process(String sql, int parameterCount) {
		if ( isProcessingNotRequired( sql ) ) {
			return sql;
		}
		return new Parser( sql, parameterCount, paramPrefix ).result();
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

		private Parser(String sql, String paramPrefix) {
			this( sql, 10, paramPrefix );
		}

		private Parser(String sql, int parameterCount, final String paramPrefix) {
			result = new StringBuilder( sql.length() + parameterCount );
			// We aren't using lambdas or method reference because of a bug in the JVM:
			// https://bugs.openjdk.java.net/browse/JDK-8161588
			// Please, don't change this unless you've tested it with Quarkus
			sql.codePoints().forEach( new IntConsumer() {
				@Override
				public void accept(int codePoint) {
					if ( escaped ) {
						escaped = false;
					}
					else {
						switch ( codePoint ) {
							case '\\':
								escaped = true;
								return;
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
									result.append( paramPrefix ).append( ++count );
									previous = '?';
									return;
								}
						}
					}
					previous = codePoint;
					result.appendCodePoint( codePoint );
				}
			} );
		}

		public String result() {
			return result.toString();
		}
	}
}
