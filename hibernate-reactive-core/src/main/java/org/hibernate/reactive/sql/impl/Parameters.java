package org.hibernate.reactive.sql.impl;

import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.PostgreSQL81Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;

import java.util.function.Supplier;

public class Parameters {

	/**
	 * Create and return a new stream of bind variables, either
	 * {@code $1, $2, $3, ...} or {@code ?, ?, ?, ...}, depending
	 * on the database of the given
	 * {@link SessionFactoryImplementor}.
	 */
	public static Supplier<String> createDialectParameterGenerator(SessionFactoryImplementor factory) {
		return createDialectParameterGenerator( factory.getJdbcServices().getDialect() );
	}

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
	public static String processParameters(String sql, SharedSessionContractImplementor session) {
		Supplier<String> generator = createDialectParameterGenerator( session.getFactory() );
		for ( int i = sql.indexOf('?'); i>=0; i = sql.indexOf('?', i+1) ) {
			sql = sql.substring(0, i) + generator.get() + sql.substring(i+1);
		}
		return sql;
	}
}
