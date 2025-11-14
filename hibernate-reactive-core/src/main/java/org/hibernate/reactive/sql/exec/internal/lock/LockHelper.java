/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal.lock;

import org.hibernate.HibernateException;
import org.hibernate.reactive.pool.ReactiveConnection;

import jakarta.persistence.Timeout;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * Reactive version of {@link org.hibernate.dialect.lock.internal.Helper}
 */
public class LockHelper {

	public static CompletionStage<Timeout> getLockTimeout(String sql, TimeoutExtractor extractor, ReactiveConnection connection) {
		return connection
				.select( sql )
				.thenApply( resultset -> {
					if ( !resultset.hasNext() ) {
						throw new HibernateException( "Unable to query JDBC Connection for current lock-timeout setting (no result)" );
					}
					return extractor.extractFrom( resultset );
				} );

	}

	/**
	 * Set the {@linkplain ReactiveConnection}-level lock-timeout using the given {@code sql} command.
	 */
	public static CompletionStage<Void> setLockTimeout(
			String sql,
			ReactiveConnection connection) {
		return connection.execute( sql );
	}

	/**
	 * Set the {@linkplain ReactiveConnection}-level lock-timeout using
	 * the given {@code sqlFormat} (with a single format placeholder
	 * for the {@code milliseconds} value).
	 *
	 * @see #setLockTimeout(String, ReactiveConnection)
	 */
	public static CompletionStage<Void> setLockTimeout(
			Integer milliseconds,
			String sqlFormat,
			ReactiveConnection connection) {
		final String sql = String.format( sqlFormat, milliseconds );
		return setLockTimeout( sql, connection );
	}

	/**
	 * Set the {@linkplain ReactiveConnection}-level lock-timeout.  The passed
	 * {@code valueStrategy} is used to interpret the {@code timeout}
	 * which is then used with {@code sqlFormat} to execute the command.
	 *
	 * @see #setLockTimeout(Integer, String, ReactiveConnection)
	 */
	public static CompletionStage<Void> setLockTimeout(
			Timeout timeout,
			Function<Timeout, Integer> valueStrategy,
			String sqlFormat,
			ReactiveConnection connection) {
		final int milliseconds = valueStrategy.apply( timeout );
		return setLockTimeout( milliseconds, sqlFormat, connection );
	}

	@FunctionalInterface
	public interface TimeoutExtractor {
		Timeout extractFrom(ReactiveConnection.Result resultSet);
	}
}
