/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.temptable;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.hibernate.dialect.temptable.TemporaryTable;
import org.hibernate.dialect.temptable.TemporaryTableExporter;
import org.hibernate.engine.jdbc.internal.FormatStyle;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.util.impl.CompletionStages;

import static org.hibernate.reactive.util.impl.CompletionStages.falseFuture;

/**
 * @see org.hibernate.dialect.temptable.TemporaryTableHelper
 */
public class ReactiveTemporaryTableHelper {
	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// Creation

	/**
	 * @see org.hibernate.jdbc.Work
	 */
	public interface ReactiveWork {
		CompletionStage<Boolean> reactiveExecute(ReactiveConnection connection);
	}

	public static class TemporaryTableCreationWork implements ReactiveWork {
		private final TemporaryTable temporaryTable;
		private final TemporaryTableExporter exporter;
		private final SessionFactoryImplementor sessionFactory;

		public TemporaryTableCreationWork(
				TemporaryTable temporaryTable,
				SessionFactoryImplementor sessionFactory) {
			this(
					temporaryTable,
					sessionFactory.getJdbcServices().getDialect().getTemporaryTableExporter(),
					sessionFactory
			);
		}

		public TemporaryTableCreationWork(
				TemporaryTable temporaryTable,
				TemporaryTableExporter exporter,
				SessionFactoryImplementor sessionFactory) {
			this.temporaryTable = temporaryTable;
			this.exporter = exporter;
			this.sessionFactory = sessionFactory;
		}

		@Override
		public CompletionStage<Boolean> reactiveExecute(ReactiveConnection connection) {
			try {
				final String creationCommand = exporter.getSqlCreateCommand( temporaryTable );

				return connection.executeUnprepared( creationCommand )
						.handle( (integer, throwable) -> {
							if ( throwable == null ) {
								return true;
							}
							logException( "create", creationCommand, temporaryTable, throwable );
							return false;
						} );
			}
			catch (Exception e) {
				logException( "create", null, temporaryTable, e );
				return falseFuture();
			}
		}
	}

	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// Drop

	public static class TemporaryTableDropWork implements ReactiveWork {
		private final TemporaryTable temporaryTable;
		private final TemporaryTableExporter exporter;
		private final SessionFactoryImplementor sessionFactory;

		public TemporaryTableDropWork(
				TemporaryTable temporaryTable,
				SessionFactoryImplementor sessionFactory) {
			this(
					temporaryTable,
					sessionFactory.getJdbcServices().getDialect().getTemporaryTableExporter(),
					sessionFactory
			);
		}

		public TemporaryTableDropWork(
				TemporaryTable temporaryTable,
				TemporaryTableExporter exporter,
				SessionFactoryImplementor sessionFactory) {
			this.temporaryTable = temporaryTable;
			this.exporter = exporter;
			this.sessionFactory = sessionFactory;
		}

		@Override
		public CompletionStage<Boolean> reactiveExecute(ReactiveConnection connection) {
			try {
				final String dropCommand = exporter.getSqlDropCommand( temporaryTable );

				return connection.update( dropCommand )
						.handle( (integer, throwable) -> {
							if ( throwable == null ) {
								return true;
							}
							logException( "drop", dropCommand, temporaryTable, throwable );
							return false;
						} );
			}
			catch (Exception e) {
				logException( "drop", null, temporaryTable, e );
				return falseFuture();
			}
		}
	}


	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// Clean

	public static CompletionStage<Void> cleanTemporaryTableRows(
			TemporaryTable temporaryTable,
			TemporaryTableExporter exporter,
			Function<SharedSessionContractImplementor, String> sessionUidAccess,
			SharedSessionContractImplementor session) {
		final String sql = exporter.getSqlTruncateCommand( temporaryTable, sessionUidAccess, session );
		Object[] params = PreparedStatementAdaptor.bind( ps -> {
			if ( temporaryTable.getSessionUidColumn() != null ) {
				final String sessionUid = sessionUidAccess.apply( session );
				ps.setString( 1, sessionUid );
			}
		} );

		return reactiveConnection( session )
				.update( sql, params )
				.thenCompose( CompletionStages::voidFuture );
	}

	private static ReactiveConnection reactiveConnection(SharedSessionContractImplementor session) {
		return ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
	}

	private static void logException(String action, String creationCommand, TemporaryTable temporaryTable, Throwable throwable) {
		if ( throwable != null ) {
			if ( creationCommand != null ) {
				// This is what ORM does
				LOG.debugf(
						"unable to " + action + " temporary table [%s]; `%s` failed : %s",
						temporaryTable.getQualifiedTableName(),
						creationCommand,
						throwable.getMessage()
				);
			}
			else {
				LOG.debugf(
						"unable to " + action + " temporary table [%s] : %s",
						temporaryTable.getQualifiedTableName(),
						throwable.getMessage()
				);
			}
		}
	}

	private static void logStatement(String sql, JdbcServices jdbcServices) {
		final SqlStatementLogger statementLogger = jdbcServices.getSqlStatementLogger();
		statementLogger.logStatement( sql, FormatStyle.BASIC.getFormatter() );
	}
}
