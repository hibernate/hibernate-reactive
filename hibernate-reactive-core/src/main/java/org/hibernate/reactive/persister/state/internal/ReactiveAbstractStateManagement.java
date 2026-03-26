/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.state.internal;

import org.hibernate.metamodel.mapping.SingularAttributeMapping;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.state.internal.AbstractStateManagement;
import org.hibernate.persister.state.spi.StateManagement;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.persister.collection.mutation.ReactiveInsertRowsCoordinator;
import org.hibernate.reactive.persister.collection.mutation.ReactiveInsertRowsCoordinatorNoOp;
import org.hibernate.reactive.persister.collection.mutation.ReactiveInsertRowsCoordinatorStandard;
import org.hibernate.reactive.persister.entity.impl.ReactiveMergeCoordinatorStandardScopeFactory;
import org.hibernate.reactive.persister.entity.impl.ReactiveUpdateCoordinatorStandardScopeFactory;
import org.hibernate.reactive.persister.entity.mutation.ReactiveDeleteCoordinator;
import org.hibernate.reactive.persister.entity.mutation.ReactiveDeleteCoordinatorStandard;
import org.hibernate.reactive.persister.entity.mutation.ReactiveInsertCoordinatorStandard;
import org.hibernate.reactive.persister.entity.mutation.ReactiveUpdateCoordinator;
import org.hibernate.reactive.persister.entity.mutation.ReactiveUpdateCoordinatorNoOp;

import static java.lang.invoke.MethodHandles.lookup;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;

/**
 * @see org.hibernate.persister.state.internal.AbstractStateManagement
 */
public abstract class ReactiveAbstractStateManagement extends AbstractStateManagement implements StateManagement {
	private static final Log LOG = make( Log.class, lookup() );

	@Override
	public ReactiveInsertCoordinatorStandard createInsertCoordinator(EntityPersister persister) {
		return new ReactiveInsertCoordinatorStandard( persister, persister.getFactory() );
	}

	@Override
	public ReactiveUpdateCoordinator createUpdateCoordinator(EntityPersister persister) {
		final var attributeMappings = persister.getAttributeMappings();
		for ( int i = 0; i < attributeMappings.size(); i++ ) {
			if ( attributeMappings.get( i ) instanceof SingularAttributeMapping ) {
				return new ReactiveUpdateCoordinatorStandardScopeFactory( persister, persister.getFactory() );
			}
		}
		return new ReactiveUpdateCoordinatorNoOp( persister );
	}

	@Override
	public ReactiveUpdateCoordinator createMergeCoordinator(EntityPersister persister) {
		return new ReactiveMergeCoordinatorStandardScopeFactory( persister, persister.getFactory() );
	}

	@Override
	public ReactiveDeleteCoordinator createDeleteCoordinator(EntityPersister persister) {
		return new ReactiveDeleteCoordinatorStandard( persister, persister.getFactory() );
	}

	@Override
	public ReactiveInsertRowsCoordinator createInsertRowsCoordinator(CollectionPersister persister) {
		final var mutationTarget = resolveMutationTarget( persister );
		if ( !isInsertAllowed( persister ) ) {
			return new ReactiveInsertRowsCoordinatorNoOp( mutationTarget );
		}
		if ( persister.isOneToMany() && isTablePerSubclass( persister ) ) {
			// See AbstractStateManagement#createInsertRowsCoordinator
			// it should be: return new ReactiveInsertRowsCoordinatorTablePerSubclass(...)
			throw LOG.notYetImplemented();
		}
		return new ReactiveInsertRowsCoordinatorStandard( mutationTarget, persister.getRowMutationOperations() );
	}
}
