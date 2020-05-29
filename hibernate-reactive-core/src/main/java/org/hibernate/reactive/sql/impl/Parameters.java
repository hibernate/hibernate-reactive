/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.impl;

import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.PostgreSQL81Dialect;

import java.util.StringTokenizer;
import java.util.function.Supplier;

public class Parameters {

	public static Supplier<String> createDialectParameterGenerator(Dialect dialect) {
		if ( dialect instanceof PostgreSQL81Dialect) { //TODO: hardcoding the dialect here is very lame
			return new Supplier<String>() {
				int count = 0;
				@Override
				public String get() {
					return "$" + (++count);
				}
			};
		}
		else {
			return () -> "?";
		}
	}

	/**
	 * Better to not use this approach.
	 */
	public static String processParameters(String sql, Dialect dialect) {
		final Supplier<String> generator = createDialectParameterGenerator( dialect );
		final StringTokenizer quoteTokenizer = new StringTokenizer( sql, "'", true );
		final StringBuilder sb = new StringBuilder();
		int quoteCount = 0;
		while ( quoteTokenizer.hasMoreTokens() ) {
			final String token = quoteTokenizer.nextToken();
			if ( token.charAt( 0 ) == '\'' ) {
				quoteCount++;
				sb.append( token );
			}
			else if ( quoteCount % 2 == 0) {
				// if quoteCount is even, that means the token is not in a quoted string
				sb.append( processParameters( token, generator ) );
			}
			else {
				// quoteCount is odd, so token is in a quoted string.
				sb.append( token );
			}
		}
		return sb.toString();
	}

	private static String processParameters(String sql, Supplier<String> generator) {
		for ( int i = sql.indexOf('?'); i>=0; i = sql.indexOf('?', i+1) ) {
			sql = sql.substring(0, i) + generator.get() + sql.substring(i+1);
		}
		return sql;
	}
}
