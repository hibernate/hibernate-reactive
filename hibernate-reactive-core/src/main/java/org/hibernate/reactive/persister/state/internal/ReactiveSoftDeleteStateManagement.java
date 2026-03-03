/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.state.internal;

import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.mutation.DeleteCoordinator;
import org.hibernate.reactive.persister.entity.mutation.ReactiveDeleteCoordinatorSoft;

public class ReactiveSoftDeleteStateManagement extends ReactiveAbstractStateManagement {
	public static final ReactiveSoftDeleteStateManagement INSTANCE = new ReactiveSoftDeleteStateManagement();

	@Override
	public DeleteCoordinator createDeleteCoordinator(EntityPersister persister) {
		return new ReactiveDeleteCoordinatorSoft( (AbstractEntityPersister) persister, persister.getFactory() );
	}

}
