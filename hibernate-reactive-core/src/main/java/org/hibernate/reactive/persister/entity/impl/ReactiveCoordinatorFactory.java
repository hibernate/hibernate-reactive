/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.impl;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metamodel.mapping.AttributeMapping;
import org.hibernate.metamodel.mapping.AttributeMappingsList;
import org.hibernate.metamodel.mapping.SingularAttributeMapping;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.reactive.persister.entity.mutation.ReactiveDeleteCoordinator;
import org.hibernate.reactive.persister.entity.mutation.ReactiveInsertCoordinator;
import org.hibernate.reactive.persister.entity.mutation.ReactiveUpdateCoordinator;
import org.hibernate.reactive.persister.entity.mutation.ReactiveUpdateCoordinatorNoOp;
import org.hibernate.reactive.persister.entity.mutation.ReactiveUpdateCoordinatorStandard;

public final class ReactiveCoordinatorFactory {

	public static ReactiveInsertCoordinator buildInsertCoordinator(
			AbstractEntityPersister entityPersister,
			SessionFactoryImplementor factory) {
		return new ReactiveInsertCoordinator( entityPersister, factory );
	}

	public static ReactiveUpdateCoordinator buildUpdateCoordinator(
			AbstractEntityPersister entityPersister,
			SessionFactoryImplementor factory) {
		// we only have updates to issue for entities with one or more singular attributes
		final AttributeMappingsList attributeMappings = entityPersister.getAttributeMappings();
		for ( int i = 0; i < attributeMappings.size(); i++ ) {
			AttributeMapping attributeMapping = attributeMappings.get( i );
			if ( attributeMapping instanceof SingularAttributeMapping ) {
				return new ReactiveUpdateCoordinatorStandard( entityPersister, factory );
			}
		}

		// otherwise, nothing to update
		return new ReactiveUpdateCoordinatorNoOp( entityPersister );
	}

	public static ReactiveDeleteCoordinator buildDeleteCoordinator(
			AbstractEntityPersister entityPersister,
			SessionFactoryImplementor factory) {
		return new ReactiveDeleteCoordinator( entityPersister, factory );
	}
}
