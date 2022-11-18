/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.impl;

import org.hibernate.reactive.provider.service.ReactiveMarkerService;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.UnknownServiceException;

/**
 * General utilities to check if certain Hibernate ORM components belong
 * to an ORM instance which is running in "Reactive mode".
 */
public final class ReactiveModeCheck {

	private ReactiveModeCheck() {
		//do not instantiate
	}

	public static boolean isReactiveRegistry(final ServiceRegistry serviceRegistry) {
		//FIXME improve how we do this check?
		try {
			serviceRegistry.requireService( ReactiveMarkerService.class );
			return true;
		}
		catch (UnknownServiceException use) {
			//This is not a reactive registry - don't register our things
			return false;
		}
	}
}
