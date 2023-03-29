/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.temptable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.hibernate.dialect.temptable.TemporaryTable;
import org.hibernate.engine.config.spi.ConfigurationService;
import org.hibernate.engine.config.spi.StandardConverters;
import org.hibernate.engine.jdbc.connections.spi.JdbcConnectionAccess;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metamodel.mapping.internal.MappingModelCreationProcess;
import org.hibernate.query.sqm.mutation.internal.temptable.PersistentTableStrategy;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;

import static java.lang.invoke.MethodHandles.lookup;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * @see org.hibernate.query.sqm.mutation.internal.temptable.PersistentTableStrategy
 */
public interface ReactivePersistentTableStrategy {

	Log LOG = make( Log.class, lookup() );

	boolean isPrepared();

	void setPrepared(boolean prepared);

	boolean isDropIdTables();

	void setDropIdTables(boolean dropIdTables);

	TemporaryTable getTemporaryTable();

	CompletionStage<Void> getDropTableActionStage();

	CompletionStage<Void> getCreateTableActionStage();

	SessionFactoryImplementor getSessionFactory();

	default void prepare(MappingModelCreationProcess mappingModelCreationProcess, JdbcConnectionAccess connectionAccess, CompletableFuture<Void> tableCreatedStage) {
		if ( isPrepared() ) {
			return;
		}

		setPrepared( true );

		final ConfigurationService configService = mappingModelCreationProcess
				.getCreationContext().getBootstrapContext()
				.getServiceRegistry().getService( ConfigurationService.class );
		boolean createIdTables = configService
				.getSetting( PersistentTableStrategy.CREATE_ID_TABLES, StandardConverters.BOOLEAN, true );

		if ( !createIdTables ) {
			tableCreatedStage.complete( null );
		}

		LOG.debugf( "Creating persistent ID table : %s", getTemporaryTable().getTableExpression() );

		connectionStage()
				.thenCompose( this::createTable )
				.whenComplete( (connection, throwable) -> releaseConnection( connection )
						.thenAccept( v -> {
							if ( throwable == null ) {
								setDropIdTables( configService );
								tableCreatedStage.complete( null );
							}
							else {
								tableCreatedStage.completeExceptionally( throwable );
							}
						} )
				);
	}

	private CompletionStage<Void> releaseConnection(ReactiveConnection connection) {
		if ( connection == null ) {
			return voidFuture();
		}
		try {
			return connection
					.close()
					.handle( (unused, t) -> {
						logConnectionClosedError( t );
						return null;
					} );
		}
		catch (Throwable t) {
			logConnectionClosedError( t );
			return voidFuture();
		}
	}

	private static void logConnectionClosedError(Throwable t) {
		if ( t != null ) {
			LOG.debugf( "Ignoring error closing the connection: %s", t.getMessage() );
		}
	}

	private CompletionStage<ReactiveConnection> createTable(ReactiveConnection connection) {
		try {
			return new ReactiveTemporaryTableHelper.TemporaryTableCreationWork(
					getTemporaryTable(),
					getSessionFactory()
			)
					.reactiveExecute( connection )
					.thenApply( v -> connection );
		}
		catch (Throwable t) {
			return failedFuture( t );
		}
	}

	private void setDropIdTables(ConfigurationService configService) {
		setDropIdTables( configService.getSetting(
				PersistentTableStrategy.DROP_ID_TABLES,
				StandardConverters.BOOLEAN,
				false
		) );
	}

	private CompletionStage<ReactiveConnection> connectionStage() {
		try {
			return getSessionFactory().getServiceRegistry()
					.getService( ReactiveConnectionPool.class )
					.getConnection();
		}
		catch (Throwable t) {
			return failedFuture( t );
		}
	}

	default void release(
			SessionFactoryImplementor sessionFactory,
			JdbcConnectionAccess connectionAccess,
			CompletableFuture<Void> tableDroppedStage) {
		if ( !isDropIdTables() ) {
			tableDroppedStage.complete( null );
		}

		setDropIdTables( false );

		final TemporaryTable temporaryTable = getTemporaryTable();
		LOG.debugf( "Dropping persistent ID table : %s", temporaryTable.getTableExpression() );

		connectionStage()
				.thenCompose( this::dropTable )
				.whenComplete( (connection, throwable) -> releaseConnection( connection )
						.thenAccept( v -> {
							if ( throwable == null ) {
								tableDroppedStage.complete( null );
							}
							else {
								tableDroppedStage.completeExceptionally( throwable );
							}
						} )
				);
	}

	private CompletionStage<ReactiveConnection> dropTable(ReactiveConnection connection) {
		try {
			return new ReactiveTemporaryTableHelper.TemporaryTableDropWork( getTemporaryTable(), getSessionFactory() )
					.reactiveExecute( connection )
					.thenApply( v -> connection );
		}
		catch (Throwable t) {
			return failedFuture( t );
		}
	}

}
