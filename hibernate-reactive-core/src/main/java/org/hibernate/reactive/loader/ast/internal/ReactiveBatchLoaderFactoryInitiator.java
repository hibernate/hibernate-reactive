/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.loader.ast.spi.BatchLoaderFactory;
import org.hibernate.service.spi.ServiceRegistryImplementor;

/**
 * Initiator for {@link ReactiveStandardBatchLoaderFactory}
 */
public class ReactiveBatchLoaderFactoryInitiator implements StandardServiceInitiator<BatchLoaderFactory> {
	/**
	 * Singleton access
	 */
	public static final ReactiveBatchLoaderFactoryInitiator INSTANCE = new ReactiveBatchLoaderFactoryInitiator();

	@Override
	public BatchLoaderFactory initiateService(Map<String, Object> configurationValues, ServiceRegistryImplementor registry) {
		return new ReactiveStandardBatchLoaderFactory();
	}

	@Override
	public Class<BatchLoaderFactory> getServiceInitiated() {
		return BatchLoaderFactory.class;
	}
}
