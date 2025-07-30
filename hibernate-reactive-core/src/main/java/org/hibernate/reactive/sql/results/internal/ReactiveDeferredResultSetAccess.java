/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.internal;

import java.lang.invoke.MethodHandles;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.JDBCException;
import org.hibernate.LockOptions;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.engine.spi.SessionEventListenerManager;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.engine.impl.ReactiveCallbackImpl;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.impl.Parameters;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.resource.jdbc.spi.JdbcSessionContext;
import org.hibernate.resource.jdbc.spi.LogicalConnectionImplementor;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcOperationQuerySelect;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.exec.spi.JdbcSelectExecutor;
import org.hibernate.sql.results.jdbc.internal.DeferredResultSetAccess;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMetadata;
import org.hibernate.type.BasicType;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.spi.TypeConfiguration;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;

public class ReactiveDeferredResultSetAccess extends DeferredResultSetAccess implements ReactiveResultSetAccess {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );
	private final SqlStatementLogger sqlStatementLogger;

	private final ExecutionContext executionContext;

	private CompletionStage<ResultSet> resultSetStage;

	private Integer columnCount;
	private ResultSet resultSet;

	public ReactiveDeferredResultSetAccess(
			JdbcOperationQuerySelect jdbcSelect,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext,
			JdbcSelectExecutor.StatementCreator statementCreator,
			int resultCountEstimate) {
		super( jdbcSelect, jdbcParameterBindings, executionContext, statementCreator, resultCountEstimate );
		this.executionContext = executionContext;
		this.sqlStatementLogger = executionContext.getSession().getJdbcServices().getSqlStatementLogger();
	}

	@Override
	public JdbcServices getJdbcServices() {
		return getFactory().getJdbcServices();
	}

	@Override
	public JDBCException convertSqlException(SQLException e, String message) {
		return getJdbcServices().getJdbcEnvironment().getSqlExceptionHelper()
				.convert( e, message );
	}

	/**
	 * Reactive version of {@link org.hibernate.sql.results.jdbc.internal.DeferredResultSetAccess#registerAfterLoadAction(ExecutionContext, LockOptions)}
	 * calling {@link ReactiveSession#reactiveLock(String, Object, LockOptions)}
	 */
	@Override
	protected void registerAfterLoadAction(ExecutionContext executionContext, LockOptions lockOptionsToUse) {
		( (ReactiveCallbackImpl) executionContext.getCallback() ).registerReactiveAfterLoadAction(
				(entity, persister, session) ->
						( (ReactiveSession) session ).reactiveLock(
								persister.getEntityName(),
								entity,
								lockOptionsToUse
						)
		);
	}

	@Override
	public ResultSet getResultSet() {
		if ( resultSet == null ) {
			throw LOG.nonReactiveMethodCall( "getReactiveResultSet" );
		}
		return resultSet;
	}

	@Override
	public CompletionStage<ResultSet> getReactiveResultSet() {
		if ( resultSetStage == null ) {
			resultSetStage = executeQuery().thenApply( this::saveResultSet );
		}
		return resultSetStage;
	}

	@Override
	public int getColumnCount() {
		// A bit of a hack
		if ( columnCount == null ) {
			throw LOG.nonReactiveMethodCall( "getReactiveColumnCount" );
		}
		return columnCount;
	}

	public CompletionStage<Integer> getReactiveColumnCount() {
		return getReactiveResultSet()
				.thenApply( ReactiveDeferredResultSetAccess::columnCount )
				.thenApply( this::saveColumnCount );
	}

	private Integer saveColumnCount(Integer columnCount) {
		this.columnCount = columnCount;
		return this.columnCount;
	}

	@Override
	public <J> BasicType<J> resolveType(
			int position,
			JavaType<J> explicitJavaType,
			SessionFactoryImplementor sessionFactory) {
		return super.resolveType( position, explicitJavaType, sessionFactory );
	}

	@Override
	public <J> BasicType<J> resolveType(
			int position,
			JavaType<J> explicitJavaType,
			TypeConfiguration typeConfiguration) {
		return super.resolveType( position, explicitJavaType, typeConfiguration );
	}

	public CompletionStage<JdbcValuesMetadata> resolveJdbcValueMetadata() {
		return getReactiveResultSet().thenApply( this::convertToMetadata );
	}

	private JdbcValuesMetadata convertToMetadata(ResultSet resultSet) {
		return (JdbcValuesMetadata) resultSet;
	}

	private static int columnCount(ResultSet resultSet) {
		try {
			return resultSet.getMetaData().getColumnCount();
		}
		catch (SQLException e) {
			throw new RuntimeException( e );
		}
	}

	private JdbcSessionContext context() {
		return executionContext.getSession().getJdbcCoordinator().getJdbcSessionOwner().getJdbcSessionContext();
	}

	private CompletionStage<ResultSet> executeQuery() {
		final LogicalConnectionImplementor logicalConnection = getPersistenceContext()
				.getJdbcCoordinator().getLogicalConnection();
		return completedFuture( logicalConnection )
				.thenCompose( lg -> {
					LOG.tracef( "Executing query to retrieve ResultSet : %s", getFinalSql() );

					Dialect dialect = executionContext.getSession().getJdbcServices().getDialect();
					// This must happen at the very last minute in order to process parameters
					// added in org.hibernate.dialect.pagination.OffsetFetchLimitHandler.processSql
					final String sql = Parameters.instance( dialect ).process( getFinalSql() );
					Object[] parameters = PreparedStatementAdaptor.bind( super::bindParameters );

					final SessionEventListenerManager eventListenerManager = executionContext
							.getSession().getEventListenerManager();

					final long executeStartNanos = executionStartNanos();

					eventListenerManager.jdbcExecuteStatementStart();
					return connection()
							.selectJdbc( sql, parameters )
							.thenCompose( this::validateResultSet )
							.whenComplete( (resultSet, throwable) -> {
								// FIXME: I don't know if this event makes sense for Vert.x
								eventListenerManager.jdbcExecuteStatementEnd();
								sqlStatementLogger.logSlowQuery( getFinalSql(), executeStartNanos, context() );
							} )
							.thenCompose( this::reactiveSkipRows )
							.handle( CompletionStages::handle )
							.thenCompose( handler -> handler.hasFailed()
									? convertException( resultSet, handler.getThrowable() )
									: handler.getResultAsCompletionStage()
							);
				} )
				// same as a finally block
				.whenComplete( (o, throwable) -> logicalConnection.afterStatement() );
	}

	private CompletionStage<ResultSet> validateResultSet(ResultSet resultSet) {
		try {
			return resultSet.getMetaData().getColumnCount() == 0
					? failedFuture( LOG.noResultException( getFinalSql() ) )
					: completedFuture( resultSet );
		}
		catch (SQLException e) {
			throw new RuntimeException( e );
		}
	}

	private ResultSet saveResultSet(ResultSet resultSet) {
		this.resultSet = resultSet;
		return saveColumnCount( resultSet );
	}

	private ResultSet saveColumnCount(ResultSet resultSet) {
		this.columnCount = columnCount( resultSet );
		saveColumnCount( columnCount );
		return resultSet;
	}

	private ReactiveConnection connection() {
		return ( (ReactiveConnectionSupplier) executionContext.getSession() ).getReactiveConnection();
	}

	private <T> CompletionStage<T> convertException(T object, Throwable throwable) {
		if ( throwable != null ) {
			Throwable cause = throwable;
			if ( throwable instanceof CompletionException ) {
				cause = throwable.getCause();
			}
			// I doubt this is ever going to happen because Vert.x is not going to throw an SQLException
			if ( cause instanceof SQLException ) {
				return failedFuture( executionContext
											 .getSession().getJdbcServices()
											 .getSqlExceptionHelper()
											 .convert(
													 (SQLException) cause,
													 "Exception executing SQL [" + getFinalSql() + "]"
											 )
				);
			}
			if ( cause instanceof HibernateException ) {
				return failedFuture( cause );
			}
			// SQL server throws an exception as soon as we run the query
			if ( cause instanceof UnsupportedOperationException && cause.getMessage().contains( "Unable to decode typeInfo for XML" ) ) {
				return failedFuture( LOG.unsupportedXmlType() );
			}
			return failedFuture( new HibernateException( cause ) );
		}
		return completedFuture( object );
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
