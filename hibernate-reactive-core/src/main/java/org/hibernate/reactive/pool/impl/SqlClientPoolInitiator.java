package org.hibernate.reactive.pool.impl;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import java.util.Map;

/**
 * A Hibernate {@link StandardServiceInitiator service initiator} that
 * integrates our {@link ReactiveConnectionPool}.
 *
 * @see ReactiveConnectionPool
 * @see SqlClientPool
 */
public class SqlClientPoolInitiator implements StandardServiceInitiator<ReactiveConnectionPool> {

	public static final SqlClientPoolInitiator INSTANCE = new SqlClientPoolInitiator();

	private SqlClientPoolInitiator() {
	}

	@Override
	public ReactiveConnectionPool initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		return new SqlClientPool();
	}

	@Override
	public Class<ReactiveConnectionPool> getServiceInitiated() {
		return ReactiveConnectionPool.class;
	}
}
