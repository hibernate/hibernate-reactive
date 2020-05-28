/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.boot.impl.marker;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.service.spi.ServiceRegistryImplementor;

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
