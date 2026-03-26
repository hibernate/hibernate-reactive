/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.state.internal;

import org.hibernate.mapping.Collection;
import org.hibernate.mapping.RootClass;
import org.hibernate.metamodel.mapping.AuxiliaryMapping;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.metamodel.mapping.internal.MappingModelCreationProcess;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.state.internal.SoftDeleteStateManagement;
import org.hibernate.reactive.persister.entity.mutation.ReactiveDeleteCoordinator;
import org.hibernate.reactive.persister.entity.mutation.ReactiveDeleteCoordinatorSoft;

/**
 * @see org.hibernate.persister.state.internal.SoftDeleteStateManagement
 */
public class ReactiveSoftDeleteStateManagement extends ReactiveAbstractStateManagement {
	public static final ReactiveSoftDeleteStateManagement INSTANCE = new ReactiveSoftDeleteStateManagement();

	private ReactiveSoftDeleteStateManagement() {}

	@Override
	public ReactiveDeleteCoordinator createDeleteCoordinator(EntityPersister persister) {
		return new ReactiveDeleteCoordinatorSoft( persister, persister.getFactory() );
	}

	@Override
	public AuxiliaryMapping createAuxiliaryMapping(EntityPersister persister, RootClass rootClass, MappingModelCreationProcess creationProcess) {
		return SoftDeleteStateManagement.INSTANCE.createAuxiliaryMapping( persister, rootClass, creationProcess);
	}

	@Override
	public AuxiliaryMapping createAuxiliaryMapping(PluralAttributeMapping pluralAttributeMapping, Collection bootDescriptor, MappingModelCreationProcess creationProcess) {
		return SoftDeleteStateManagement.INSTANCE.createAuxiliaryMapping( pluralAttributeMapping, bootDescriptor, creationProcess );
	}
}
