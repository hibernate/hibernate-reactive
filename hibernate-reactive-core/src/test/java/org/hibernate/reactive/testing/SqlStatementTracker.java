/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.testing;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Predicate;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.engine.jdbc.internal.Formatter;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.service.spi.ServiceRegistryImplementor;

/**
 * Track sql queries.
 * <p>
 * Check {@link #registerService(StandardServiceRegistryBuilder)} to register an instance of this class.
 * It's not thread-safe.
 * </p>
 */
public class SqlStatementTracker extends SqlStatementLogger {

	private final Predicate<String> filter;

	private final List<String> statements = new ArrayList<>();

	public SqlStatementTracker(Predicate<String> predicate) {
		this.filter = predicate;
	}

	public SqlStatementTracker(Predicate<String> predicate, Properties properties) {
		super(
				toBoolean( properties.get( Settings.SHOW_SQL ) ),
				toBoolean( properties.get( Settings.FORMAT_SQL ) ),
				toBoolean( properties.get( Settings.HIGHLIGHT_SQL ) ),
				toLong( properties.get( Settings.LOG_SLOW_QUERY ) )
		);
		this.filter = predicate;
	}

	private static boolean toBoolean(Object obj) {
		return Boolean.parseBoolean( (String) obj );
	}

	private static long toLong(Object obj) {
		if ( obj == null ) {
			return 0;
		}
		return Long.parseLong( (String) obj );
	}

	@Override
	public void logStatement(String statement, Formatter formatter) {
		addSql( statement );
		super.logStatement( statement, formatter );
	}

	private void addSql(String sql) {
		if ( filter.test( sql ) ) {
			statements.add( sql );
		}
	}

	public List<String> getLoggedQueries() {
		return statements;
	}

	public void clear() {
		statements.clear();
	}

	/**
	 * Register the current sql tracker to the {@link StandardServiceRegistryBuilder}.
	 *
	 * @param builder the {@link StandardServiceRegistryBuilder} for the creation of the factory
	 */
	public void registerService(StandardServiceRegistryBuilder builder) {
		Initiator initiator = new Initiator( this );
		builder.addInitiator( initiator );
	}

	private static class Initiator implements StandardServiceInitiator<SqlStatementLogger> {
		private final SqlStatementTracker sqlStatementTracker;

		public Initiator(SqlStatementTracker sqlStatementTracker) {
			this.sqlStatementTracker = sqlStatementTracker;
		}

		@Override
		public SqlStatementLogger initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
			return sqlStatementTracker;
		}

		@Override
		public Class<SqlStatementLogger> getServiceInitiated() {
			return SqlStatementLogger.class;
		}
	}

//	private static class JdbcServicesLogger extends JdbcServicesImpl {
//		private final SqlStatementTracker tracker;
//
//		public JdbcServicesLogger(SqlStatementTracker tracker) {
//			this.tracker = tracker;
//		}
//
//		@Override
//		public SqlStatementLogger getSqlStatementLogger() {
//			return tracker;
//		}
//	}
}
