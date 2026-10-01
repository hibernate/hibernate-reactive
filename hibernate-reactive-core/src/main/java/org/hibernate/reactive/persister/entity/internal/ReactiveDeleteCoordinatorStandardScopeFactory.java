/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.internal;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.mutation.DeleteCoordinatorStandard;
import org.hibernate.reactive.persister.entity.mutation.ReactiveDeleteCoordinator;
import org.hibernate.reactive.persister.entity.mutation.ReactiveDeleteCoordinatorStandard;
import org.hibernate.reactive.persister.entity.mutation.ReactiveScopedDeleteCoordinator;

public final class ReactiveDeleteCoordinatorStandardScopeFactory extends DeleteCoordinatorStandard implements ReactiveDeleteCoordinator {

	public ReactiveDeleteCoordinatorStandardScopeFactory(
			EntityPersister entityPersister,
			SessionFactoryImplementor factory) {
		super( entityPersister, factory );
	}

	@Override
	public ReactiveScopedDeleteCoordinator makeScopedCoordinator() {
		return new ReactiveDeleteCoordinatorStandard(
				entityPersister(),
				factory()
		);
	}

}
