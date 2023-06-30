/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.impl;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.mutation.UpdateCoordinatorStandard;
import org.hibernate.reactive.persister.entity.mutation.ReactiveScopedUpdateCoordinator;
import org.hibernate.reactive.persister.entity.mutation.ReactiveUpdateCoordinator;
import org.hibernate.reactive.persister.entity.mutation.ReactiveUpdateCoordinatorStandard;

public class ReactiveUpdateCoordinatorStandardScopeFactory extends UpdateCoordinatorStandard implements ReactiveUpdateCoordinator {

	private final AbstractEntityPersister entityPersister;
	private final SessionFactoryImplementor factory;

	public ReactiveUpdateCoordinatorStandardScopeFactory(
			AbstractEntityPersister entityPersister,
			SessionFactoryImplementor factory) {
		super( entityPersister, factory );
		this.entityPersister = entityPersister;
		this.factory = factory;
	}

	@Override
	public ReactiveScopedUpdateCoordinator makeScopedCoordinator() {
		return new ReactiveUpdateCoordinatorStandard( entityPersister, factory );
	}

}
