package org.hibernate.reactive.service.initiator;

import org.hibernate.reactive.service.ReactiveConnection;
import org.hibernate.reactive.service.ReactiveConnectionPoolProviderImpl;
import org.hibernate.service.Service;

import java.util.concurrent.CompletionStage;

/**
 * A Hibernate {@link Service} that provides access to pooled
 * {@link ReactiveConnection reactive connections}.
 *
 * @see ReactiveConnectionPoolProviderImpl
 */
public interface ReactiveConnectionPoolProvider extends Service {

	/**
	 * Obtain a reactive connection.
	 */
	CompletionStage<ReactiveConnection> getConnection();

	void close();
}
