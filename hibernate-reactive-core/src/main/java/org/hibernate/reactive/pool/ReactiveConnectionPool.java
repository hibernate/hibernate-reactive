package org.hibernate.reactive.pool;

import org.hibernate.service.Service;

import java.util.concurrent.CompletionStage;

/**
 * A Hibernate {@link Service} that provides access to pooled
 * {@link ReactiveConnection reactive connections}.
 *
 * A program may integrate a custom {@link ReactiveConnectionPool}
 * with Hibernate Reactive by contributing a new service using a
 * {@link org.hibernate.boot.registry.StandardServiceInitiator}
 * or from code-based Hibernate configuration by calling
 * {@link org.hibernate.boot.registry.StandardServiceRegistryBuilder#addService}.
 */
public interface ReactiveConnectionPool extends Service {

	/**
	 * Obtain a reactive connection.
	 */
	CompletionStage<ReactiveConnection> getConnection();

	void close();
}
