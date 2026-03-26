/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.impl;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metamodel.mapping.SoftDeleteMapping;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.mutation.DeleteCoordinator;
import org.hibernate.reactive.persister.entity.mutation.ReactiveDeleteCoordinatorStandard;
import org.hibernate.reactive.persister.entity.mutation.ReactiveDeleteCoordinatorSoft;
import org.hibernate.reactive.persister.entity.mutation.ReactiveInsertCoordinatorStandard;
import org.hibernate.reactive.persister.entity.mutation.ReactiveUpdateCoordinator;
import org.hibernate.reactive.persister.state.internal.ReactiveStandardStateManagement;

public final class ReactiveCoordinatorFactory {

	public static ReactiveInsertCoordinatorStandard buildInsertCoordinator(EntityPersister entityPersister) {
		return ReactiveStandardStateManagement.INSTANCE.createInsertCoordinator( entityPersister );
	}

	public static ReactiveUpdateCoordinator buildUpdateCoordinator(EntityPersister entityPersister) {
		return ReactiveStandardStateManagement.INSTANCE.createUpdateCoordinator( entityPersister );
	}

	public static DeleteCoordinator buildDeleteCoordinator(SoftDeleteMapping softDeleteMapping, AbstractEntityPersister entityPersister, SessionFactoryImplementor factory) {
		return softDeleteMapping != null
				? new ReactiveDeleteCoordinatorSoft( entityPersister, factory )
				: new ReactiveDeleteCoordinatorStandard( entityPersister, factory );
	}

	public static ReactiveUpdateCoordinator buildMergeCoordinator(AbstractEntityPersister entityPersister) {
		return ReactiveStandardStateManagement.INSTANCE.createMergeCoordinator( entityPersister );
	}
}
