/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.internal;

import java.lang.invoke.MethodHandles;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.hibernate.HibernateException;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.engine.spi.SessionEventListenerManager;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.impl.Parameters;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.resource.jdbc.spi.LogicalConnectionImplementor;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.exec.spi.JdbcSelect;
import org.hibernate.sql.results.jdbc.internal.DeferredResultSetAccess;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;

public class ReactiveDeferredResultSetAccess extends DeferredResultSetAccess implements ReactiveResultSetAccess {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );
	private final SqlStatementLogger sqlStatementLogger;
	private final Function<String, PreparedStatement> statementCreator;

	private final ExecutionContext executionContext;

	private CompletionStage<ResultSet> resultSetStage;

	public ReactiveDeferredResultSetAccess(
			JdbcSelect jdbcSelect,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext,
			Function<String, PreparedStatement> statementCreator) {
		super( jdbcSelect, jdbcParameterBindings, executionContext, statementCreator );
		this.executionContext = executionContext;
		this.sqlStatementLogger = executionContext.getSession().getJdbcServices().getSqlStatementLogger();
		this.statementCreator = statementCreator;
	}

	@Override
	public ResultSet getResultSet() {
		throw new UnsupportedOperationException("Not reactive");
	}

	@Override
	public CompletionStage<ResultSet> getReactiveResultSet() {
		if ( resultSetStage == null ) {
			resultSetStage = executeQuery();
		}
		return resultSetStage;
	}

	private CompletionStage<ResultSet> executeQuery() {
		final LogicalConnectionImplementor logicalConnection = getPersistenceContext().getJdbcCoordinator().getLogicalConnection();
		return completedFuture( logicalConnection )
				.thenCompose( lg -> {
					LOG.tracef( "Executing query to retrieve ResultSet : %s", getFinalSql() );

					Dialect dialect = executionContext.getSession().getJdbcServices().getDialect();
					final String sql = Parameters.instance( dialect ).process( getFinalSql() );
					Object[] parameters = PreparedStatementAdaptor.bind( super::bindParameters );

					final SessionEventListenerManager eventListenerManager = executionContext
							.getSession().getEventListenerManager();

					final long executeStartNanos = executionStartNanos();

					eventListenerManager.jdbcExecuteStatementStart();
					return connection()
							.selectJdbc( sql, parameters )
							.whenComplete( (resultSet, throwable) -> {
								// FIXME: I don't know if this event makes sense for Vert.x
								eventListenerManager.jdbcExecuteStatementEnd();
								sqlStatementLogger.logSlowQuery( getFinalSql(), executeStartNanos );
							} )
							.thenCompose( this::reactiveSkipRows )
							.handle( this::convertException );
				} )
				.whenComplete( (o, throwable) -> logicalConnection.afterStatement() );
	}

	private ReactiveConnection connection() {
		return ( (ReactiveConnectionSupplier) executionContext.getSession() ).getReactiveConnection();
	}

	private ResultSet convertException(ResultSet resultSet, Throwable throwable) {
		// FIXME: Vert.x will probably throw another exception. Check this.
		if ( throwable instanceof SQLException) {
			throw executionContext.getSession().getJdbcServices()
					.getSqlExceptionHelper()
					// FIXME: Add this to the logger?
					.convert( (SQLException) throwable, "Exception executing SQL [" + getFinalSql() + "]" );
		}
		if ( throwable != null ) {
			throw new HibernateException( throwable );
		}
		return resultSet;
	}

	private CompletionStage<ResultSet> reactiveSkipRows(ResultSet resultSet) {
		try {
			skipRows( resultSet );
			return completedFuture( resultSet );
		}
		catch (SQLException sqlException) {
			// I don't think this will happen, but just in case ...
			return failedFuture( sqlException );
		}
	}

	private long executionStartNanos() {
		return this.sqlStatementLogger.getLogSlowQuery() > 0
				? System.nanoTime()
				: 0;
	}
}
