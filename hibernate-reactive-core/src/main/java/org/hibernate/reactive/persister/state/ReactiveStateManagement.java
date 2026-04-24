/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.state;

import org.hibernate.metamodel.mapping.AuxiliaryMapping;
import org.hibernate.metamodel.mapping.internal.MappingModelCreationProcess;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.mapping.Collection;
import org.hibernate.mapping.RootClass;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.collection.mutation.DeleteRowsCoordinator;
import org.hibernate.persister.collection.mutation.InsertRowsCoordinator;
import org.hibernate.persister.collection.mutation.RemoveCoordinator;
import org.hibernate.persister.collection.mutation.UpdateRowsCoordinator;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.mutation.DeleteCoordinator;
import org.hibernate.persister.entity.mutation.InsertCoordinator;
import org.hibernate.persister.entity.mutation.UpdateCoordinator;
import org.hibernate.persister.state.internal.StandardStateManagement;
import org.hibernate.persister.state.spi.StateManagement;
import org.hibernate.reactive.persister.entity.impl.ReactiveCoordinatorFactory;

/**
 * Reactive implementation of {@link StateManagement} that creates reactive coordinators
 * for entity and collection mutation operations.
 */
public final class ReactiveStateManagement implements StateManagement {

	public static final ReactiveStateManagement INSTANCE = new ReactiveStateManagement();

	private ReactiveStateManagement() {
	}

	@Override
	public InsertCoordinator createInsertCoordinator(EntityPersister persister) {
		if ( persister instanceof AbstractEntityPersister aep ) {
			return ReactiveCoordinatorFactory.buildInsertCoordinator( aep, aep.getFactory() );
		}
		return StandardStateManagement.INSTANCE.createInsertCoordinator( persister );
	}

	@Override
	public UpdateCoordinator createUpdateCoordinator(EntityPersister persister) {
		if ( persister instanceof AbstractEntityPersister aep ) {
			return ReactiveCoordinatorFactory.buildUpdateCoordinator( aep, aep.getFactory() );
		}
		return StandardStateManagement.INSTANCE.createUpdateCoordinator( persister );
	}

	@Override
	public UpdateCoordinator createMergeCoordinator(EntityPersister persister) {
		if ( persister instanceof AbstractEntityPersister aep ) {
			return ReactiveCoordinatorFactory.buildMergeCoordinator( aep, aep.getFactory() );
		}
		return StandardStateManagement.INSTANCE.createMergeCoordinator( persister );
	}

	@Override
	public DeleteCoordinator createDeleteCoordinator(EntityPersister persister) {
		if ( persister instanceof AbstractEntityPersister aep ) {
			return ReactiveCoordinatorFactory.buildDeleteCoordinator( aep.getSoftDeleteMapping(), aep, aep.getFactory() );
		}
		return StandardStateManagement.INSTANCE.createDeleteCoordinator( persister );
	}

	@Override
	public InsertRowsCoordinator createInsertRowsCoordinator(CollectionPersister persister) {
		return StandardStateManagement.INSTANCE.createInsertRowsCoordinator( persister );
	}

	@Override
	public UpdateRowsCoordinator createUpdateRowsCoordinator(CollectionPersister persister) {
		return StandardStateManagement.INSTANCE.createUpdateRowsCoordinator( persister );
	}

	@Override
	public DeleteRowsCoordinator createDeleteRowsCoordinator(CollectionPersister persister) {
		return StandardStateManagement.INSTANCE.createDeleteRowsCoordinator( persister );
	}

	@Override
	public RemoveCoordinator createRemoveCoordinator(CollectionPersister persister) {
		return StandardStateManagement.INSTANCE.createRemoveCoordinator( persister );
	}

	@Override
	public AuxiliaryMapping createAuxiliaryMapping(
			EntityPersister persister,
			RootClass rootClass,
			MappingModelCreationProcess creationProcess) {
		return StandardStateManagement.INSTANCE.createAuxiliaryMapping( persister, rootClass, creationProcess );
	}

	@Override
	public AuxiliaryMapping createAuxiliaryMapping(
			PluralAttributeMapping pluralAttributeMapping,
			Collection bootDescriptor,
			MappingModelCreationProcess creationProcess) {
		return StandardStateManagement.INSTANCE.createAuxiliaryMapping( pluralAttributeMapping, bootDescriptor, creationProcess );
	}
}
