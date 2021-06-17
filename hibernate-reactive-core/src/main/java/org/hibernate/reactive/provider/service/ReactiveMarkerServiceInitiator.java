/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.service.spi.ServiceRegistryImplementor;

/**
 * A Hibernate {@link StandardServiceInitiator service initiator} for {@link ReactiveMarkerService}.
 */
public final class ReactiveMarkerServiceInitiator implements StandardServiceInitiator<ReactiveMarkerService> {

	public static final ReactiveMarkerServiceInitiator INSTANCE = new ReactiveMarkerServiceInitiator();

	private ReactiveMarkerServiceInitiator(){}

	@Override
	public ReactiveMarkerService initiateService(final Map configurationValues, final ServiceRegistryImplementor registry) {
		return ReactiveMarkerServiceSingleton.INSTANCE;
	}

	@Override
	public Class<ReactiveMarkerService> getServiceInitiated() {
		return ReactiveMarkerService.class;
	}
}
