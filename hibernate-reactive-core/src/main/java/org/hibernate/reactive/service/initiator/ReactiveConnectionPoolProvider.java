package org.hibernate.reactive.service.initiator;

import org.hibernate.reactive.service.ReactiveConnection;
import org.hibernate.reactive.service.ReactiveConnectionPoolProviderImpl;
import org.hibernate.service.Service;

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
	ReactiveConnection getConnection();

	void close();
}
