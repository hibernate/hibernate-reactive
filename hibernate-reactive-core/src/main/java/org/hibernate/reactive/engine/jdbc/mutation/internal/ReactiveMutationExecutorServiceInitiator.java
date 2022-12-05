/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.mutation.internal;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.engine.jdbc.mutation.internal.MutationExecutorServiceInitiator;
import org.hibernate.engine.jdbc.mutation.spi.MutationExecutorService;
import org.hibernate.reactive.sql.exec.internal.ReactiveStandardMutationExecutorService;
import org.hibernate.service.spi.ServiceRegistryImplementor;

public class ReactiveMutationExecutorServiceInitiator implements StandardServiceInitiator<MutationExecutorService> {

	public static final ReactiveMutationExecutorServiceInitiator INSTANCE = new ReactiveMutationExecutorServiceInitiator();

	@Override
	public Class<MutationExecutorService> getServiceInitiated() {
		return MutationExecutorService.class;
	}

	/**
	 * @see MutationExecutorServiceInitiator#initiateService(Map, ServiceRegistryImplementor)
	 */
	@Override
	public MutationExecutorService initiateService(Map<String, Object> configurationValues, ServiceRegistryImplementor registry) {
		final Object custom = configurationValues.get( MutationExecutorServiceInitiator.EXECUTOR_KEY );

		return custom == null
				? createStandardService( configurationValues )
				: MutationExecutorServiceInitiator.INSTANCE.initiateService( configurationValues, registry );
	}

	private static MutationExecutorService createStandardService(Map<String, Object> configurationValues) {
		return new ReactiveStandardMutationExecutorService( configurationValues );
	}
}
