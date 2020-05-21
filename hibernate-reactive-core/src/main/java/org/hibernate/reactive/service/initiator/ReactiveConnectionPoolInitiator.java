package org.hibernate.reactive.service.initiator;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.reactive.service.ReactiveConnectionPool;
import org.hibernate.reactive.service.SqlClientPool;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import java.util.Map;

/**
 * A Hibernate {@link StandardServiceInitiator service initiator} that
 * integrates our {@link ReactiveConnectionPool}.
 *
 * @see ReactiveConnectionPool
 * @see SqlClientPool
 */
public class ReactiveConnectionPoolInitiator implements StandardServiceInitiator<SqlClientPool> {

	public static final ReactiveConnectionPoolInitiator INSTANCE = new ReactiveConnectionPoolInitiator();

	private ReactiveConnectionPoolInitiator() {
	}

	@Override
	public SqlClientPool initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		return new SqlClientPool( configurationValues );
	}

	@Override
	public Class<SqlClientPool> getServiceInitiated() {
		return SqlClientPool.class;
	}
}
