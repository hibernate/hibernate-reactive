/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import java.util.concurrent.CompletionStage;

import org.hibernate.Incubating;
import org.hibernate.reactive.context.Context;
import org.hibernate.reactive.context.impl.BaseKey;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveTransactionCoordinator;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * A connection pool that stores connections in the Vert.x context for
 * external transaction management.
 * <p>
 * This pool is designed for frameworks like Quarkus that manage transactions
 * externally (similar to JTA). It:
 * <ul>
 *     <li>Stores connections in the Vert.x {@link Context}</li>
 *     <li>Returns the same connection for all requests within the same context</li>
 *     <li>Wraps connections with {@link ExternallyManagedConnection} for proper
 *         transaction coordination</li>
 *     <li>Uses {@link ExternalTransactionCoordinator} to handle transaction
 *         synchronization callbacks</li>
 * </ul>
 * <p>
 * Subclasses or extensions can customize behavior by overriding methods or
 * providing a custom connection key.
 *
 * @see ExternalTransactionCoordinator
 * @see ExternallyManagedConnection
 */
@Incubating
public class ContextualConnectionPool extends DefaultSqlClientPool {

	private static final Context.Key<ExternallyManagedConnection> CONNECTION_KEY =
			new BaseKey<>( ExternallyManagedConnection.class, "org.hibernate.reactive.external.connection" );

	private Context context;
	private final ExternalTransactionCoordinator coordinator;

	public ContextualConnectionPool() {
		this.coordinator = new ExternalTransactionCoordinator( this::getConnectionFromContext );
	}

	@Override
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
		super.injectServices( serviceRegistry );
		this.context = serviceRegistry.getService( Context.class );
	}

	/**
	 * Returns the context used for storing connections.
	 *
	 * @return the Vert.x context
	 */
	protected Context getContext() {
		return context;
	}

	/**
	 * Returns the key used to store connections in the context.
	 * Subclasses can override to provide a custom key.
	 *
	 * @return the context key for connections
	 */
	protected Context.Key<ExternallyManagedConnection> getConnectionKey() {
		return CONNECTION_KEY;
	}

	@Override
	public ReactiveTransactionCoordinator getTransactionCoordinator() {
		return coordinator;
	}

	/**
	 * Gets the connection from the context, if one exists.
	 *
	 * @return the connection from context, or {@code null} if none exists
	 */
	private CompletionStage<ReactiveConnection> getConnectionFromContext() {
		ExternallyManagedConnection connection = context.get( getConnectionKey() );
		return connection != null ? completedFuture( connection ) : null;
	}

	/**
	 * Closes the underlying connection and removes it from context.
	 * Called by the external transaction manager after commit/rollback.
	 *
	 * @return a completion stage that completes when the connection is closed
	 */
	public CompletionStage<Void> closeConnection() {
		ExternallyManagedConnection connection = context.get( getConnectionKey() );
		context.remove( getConnectionKey() );
		return connection != null ? connection.closeUnderlyingConnection() : voidFuture();
	}

	@Override
	public CompletionStage<Void> getCloseFuture() {
		return closeConnection()
				.thenCompose( v -> super.getCloseFuture() );
	}

	@Override
	public CompletionStage<ReactiveConnection> getConnection() {
		// Check context first - return existing connection if present
		ExternallyManagedConnection existing = context.get( getConnectionKey() );
		if ( existing != null ) {
			return completedFuture( existing );
		}

		// No existing connection - create, wrap, and store in context
		return super.getConnection()
				.thenApply( connection -> {
					ExternallyManagedConnection wrapped = ExternallyManagedConnection.wrap( connection, coordinator );
					context.put( getConnectionKey(), wrapped );
					return wrapped;
				} );
	}

	@Override
	public ReactiveConnection getProxyConnection() {
		// Check context first - return existing connection if present
		ExternallyManagedConnection existing = context.get( getConnectionKey() );
		if ( existing != null ) {
			return existing;
		}
		ReactiveConnection proxyConnection = super.getProxyConnection();
		// No existing connection - create, wrap, and store in context
		ExternallyManagedConnection wrapped = ExternallyManagedConnection.wrap( proxyConnection, coordinator );
		context.put( getConnectionKey(), wrapped );
		return wrapped;
	}
}
