package org.hibernate.reactive.service.initiator;

import org.hibernate.reactive.service.RxConnection;
import org.hibernate.service.Service;

/**
 * A Hibernate {@link Service} that provides access to pooled
 * {@link RxConnection reactive connections}.
 *
 * @see org.hibernate.reactive.service.RxConnectionPoolProviderImpl
 */
public interface RxConnectionPoolProvider extends Service {

	/**
	 * Obtain a reactive connection.
	 */
	RxConnection getConnection();

	void close();
}
