package org.hibernate.reactive.service;

import org.hibernate.service.Service;

import java.util.concurrent.CompletionStage;

/**
 * A Hibernate {@link Service} that provides access to pooled
 * {@link ReactiveConnection reactive connections}.
 *
 * @see SqlClientPool
 */
public interface ReactiveConnectionPool extends Service {

	/**
	 * Obtain a reactive connection.
	 */
	CompletionStage<ReactiveConnection> getConnection();

	void close();
}
