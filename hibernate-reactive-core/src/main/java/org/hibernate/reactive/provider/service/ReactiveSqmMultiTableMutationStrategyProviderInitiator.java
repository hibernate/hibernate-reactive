/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableMutationStrategyProvider;
import org.hibernate.service.spi.ServiceRegistryImplementor;

/**
 * Custom variations for SqmMultiTableMutationStrategyProvider
 */
public class ReactiveSqmMultiTableMutationStrategyProviderInitiator
		implements StandardServiceInitiator<SqmMultiTableMutationStrategyProvider> {
	/**
	 * Singleton access
	 */
	public static final ReactiveSqmMultiTableMutationStrategyProviderInitiator INSTANCE = new ReactiveSqmMultiTableMutationStrategyProviderInitiator();

	@Override
	public SqmMultiTableMutationStrategyProvider initiateService(
			Map<String, Object> configurationValues,
			ServiceRegistryImplementor registry) {
		return new ReactiveSqmMultiTableMutationStrategyProvider();
	}

	@Override
	public Class<SqmMultiTableMutationStrategyProvider> getServiceInitiated() {
		return SqmMultiTableMutationStrategyProvider.class;
	}
}
