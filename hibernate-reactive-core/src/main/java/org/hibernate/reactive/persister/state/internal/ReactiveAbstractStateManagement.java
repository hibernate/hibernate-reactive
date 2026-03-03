/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.state.internal;

import org.hibernate.metamodel.mapping.SingularAttributeMapping;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.mutation.DeleteCoordinator;
import org.hibernate.persister.entity.mutation.InsertCoordinator;
import org.hibernate.persister.entity.mutation.UpdateCoordinator;
import org.hibernate.persister.state.internal.AbstractStateManagement;
import org.hibernate.reactive.persister.entity.impl.ReactiveMergeCoordinatorStandardScopeFactory;
import org.hibernate.reactive.persister.entity.impl.ReactiveUpdateCoordinatorStandardScopeFactory;
import org.hibernate.reactive.persister.entity.mutation.ReactiveDeleteCoordinator;
import org.hibernate.reactive.persister.entity.mutation.ReactiveInsertCoordinatorStandard;
import org.hibernate.reactive.persister.entity.mutation.ReactiveUpdateCoordinatorNoOp;

public abstract class ReactiveAbstractStateManagement extends AbstractStateManagement {

	@Override
	public InsertCoordinator createInsertCoordinator(EntityPersister persister) {
		return new ReactiveInsertCoordinatorStandard( (AbstractEntityPersister) persister, persister.getFactory() );
	}

	@Override
	public UpdateCoordinator createUpdateCoordinator(EntityPersister persister) {
		final var attributeMappings = persister.getAttributeMappings();
		for ( int i = 0; i < attributeMappings.size(); i++ ) {
			if ( attributeMappings.get( i ) instanceof SingularAttributeMapping ) {
				return new ReactiveUpdateCoordinatorStandardScopeFactory(
						(AbstractEntityPersister) persister,
						persister.getFactory()
				);
			}
		}
		return new ReactiveUpdateCoordinatorNoOp( (AbstractEntityPersister) persister );
	}

	@Override
	public UpdateCoordinator createMergeCoordinator(EntityPersister persister) {
		return new ReactiveMergeCoordinatorStandardScopeFactory( (AbstractEntityPersister) persister, persister.getFactory() );
	}

	@Override
	public DeleteCoordinator createDeleteCoordinator(EntityPersister persister) {
		return new ReactiveDeleteCoordinator( (AbstractEntityPersister) persister, persister.getFactory() );
	}

}
