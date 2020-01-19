package org.hibernate.rx.sql.impl;

import org.hibernate.dialect.PostgreSQL81Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;

import java.util.function.Supplier;

public class Parameters {

	/**
	 * Create and return a new stream of bind variables, either
	 * {@code $1, $2, $3, ...} or {@code ?, ?, ?, ...}, depending
	 * on the database of the given
	 * {@link SessionFactoryImplementor}.
	 */
	public static Supplier<String> createDialectParameterGenerator(SessionFactoryImplementor factory) {
		//TODO: hardcoding the dialect here is very lame
		if ( factory.getJdbcServices().getDialect() instanceof PostgreSQL81Dialect) {
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

}
