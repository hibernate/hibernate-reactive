package org.hibernate.rx.service.initiator;

import org.hibernate.rx.service.RxConnection;
import org.hibernate.service.Service;

/**
 * A Hibernate {@link Service} that provides access to pooled
 * {@link RxConnection reactive connections}.
 *
 * @see org.hibernate.rx.service.RxConnectionPoolProviderImpl
 */
public interface RxConnectionPoolProvider extends Service {

	/**
	 * Obtain a reactive connection.
	 */
	RxConnection getConnection();

	void close();
}
