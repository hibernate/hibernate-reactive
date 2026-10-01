/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.internal;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.mutation.DeleteCoordinatorSoft;
import org.hibernate.reactive.persister.entity.mutation.ReactiveDeleteCoordinator;
import org.hibernate.reactive.persister.entity.mutation.ReactiveDeleteCoordinatorSoft;
import org.hibernate.reactive.persister.entity.mutation.ReactiveScopedDeleteCoordinator;

public final class ReactiveDeleteCoordinatorSoftScopeFactory extends DeleteCoordinatorSoft implements ReactiveDeleteCoordinator {

	public ReactiveDeleteCoordinatorSoftScopeFactory(
			EntityPersister entityPersister,
			SessionFactoryImplementor factory) {
		super( entityPersister, factory );
	}

	@Override
	public ReactiveScopedDeleteCoordinator makeScopedCoordinator() {
		return new ReactiveDeleteCoordinatorSoft(
				entityPersister(),
				factory()
		);
	}

}
