/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool;

import org.hibernate.Incubating;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.service.Service;

import java.sql.ResultSet;
import java.util.concurrent.CompletionStage;


/**
 * A Hibernate {@link Service} that provides access to pooled
 * {@link ReactiveConnection reactive connections}.
 * <p>
 * A custom implementation of {@link ReactiveConnectionPool}
 * may be selected by setting the configuration property
 * {@link org.hibernate.reactive.provider.Settings#SQL_CLIENT_POOL}.
 * <p>
 * Alternatively, a program may integrate a custom
 * {@code ReactiveConnectionPool} by contributing a new service using
 * a {@link org.hibernate.boot.registry.StandardServiceInitiator}
 * or from code-based Hibernate configuration by calling
 * {@link ReactiveServiceRegistryBuilder#addService}.
 *
 * <pre>
 * new ReactiveServiceRegistryBuilder()
 *     .applySettings( properties )
 *     .addService( ReactiveConnectionPool.class, new MyReactiveConnectionPool() )
 *     .build();
 * </pre>
 */
@Incubating
public interface ReactiveConnectionPool extends Service {
	/**
	 * Obtain a lazily-initializing reactive connection. The
	 * actual connection might be made when the returned
	 * instance if {@link ReactiveConnection} is first used.
	 */
	ReactiveConnection getProxyConnection();

	/**
	 * Obtain a reactive connection, returning the connection
	 * via a {@link CompletionStage} and overriding the default
	 * {@link SqlExceptionHelper} for the pool.
	 */
	ReactiveConnection getProxyConnection(SqlExceptionHelper sqlExceptionHelper);

	/**
	 * Obtain a reactive connection for the given tenant id,
	 * returning the connection via a {@link CompletionStage}.
	 */
	ReactiveConnection getProxyConnection(String tenantId);

	/**
	 * Obtain a reactive connection, returning the connection
	 * via a {@link CompletionStage} and overriding the default
	 * {@link SqlExceptionHelper} for the pool.
	 */
	CompletionStage<ReactiveConnection> getConnection(SqlExceptionHelper sqlExceptionHelper);

	/**
	 * Obtain a reactive connection for the given tenant id,
	 * returning the connection via a {@link CompletionStage}.
	 */
	CompletionStage<ReactiveConnection> getConnection(String tenantId);

	/**
	 * Obtain a reactive connection, returning the connection
	 * via a {@link CompletionStage}.
	 */
	CompletionStage<ReactiveConnection> getConnection();

	/**
	 * Obtain a reactive connection for the given tenant id,
	 * returning the connection via a {@link CompletionStage}
	 * and overriding the default {@link SqlExceptionHelper}
	 * for the pool.
	 */
	CompletionStage<ReactiveConnection> getConnection(String tenantId, SqlExceptionHelper sqlExceptionHelper);

	CompletionStage<ResultSet> selectJdbcOutsideTransaction(String sql, Object[] paramValues);

	/**
	 * The shutdown of the pool is actually asynchronous but the
	 * core service registry won't return the {@link CompletionStage}.
	 * If you need to wait for the underlying pool to be closed,
	 * after closing the SessionFactory you can get the CompletionStage
	 * instance from this getter.
	 */
	CompletionStage<Void> getCloseFuture();
}
