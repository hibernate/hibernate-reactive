/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.metamodel.spi;

import org.hibernate.mapping.PersistentClass;
import org.hibernate.metamodel.internal.EntityRepresentationStrategyMap;
import org.hibernate.metamodel.internal.ManagedTypeRepresentationResolverStandard;
import org.hibernate.metamodel.spi.EntityRepresentationStrategy;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.metamodel.internal.ReactiveEntityRepresentationStrategyPojoStandard;

/**
 * Extends {@link ManagedTypeRepresentationResolverStandard} to create a {@link ReactiveEntityRepresentationStrategyPojoStandard}
 */
public class ReactiveManagedTypeRepresentationResolver extends ManagedTypeRepresentationResolverStandard {

	public static final ManagedTypeRepresentationResolverStandard INSTANCE = new ReactiveManagedTypeRepresentationResolver();

	@Override
	public EntityRepresentationStrategy resolveStrategy(
			PersistentClass bootDescriptor,
			EntityPersister runtimeDescriptor,
			RuntimeModelCreationContext creationContext) {
		if ( bootDescriptor.getMappedClass() == null ) { // i.e. RepresentationMode.MAP;
			return new EntityRepresentationStrategyMap( bootDescriptor, creationContext );
		}
		else {
			return new ReactiveEntityRepresentationStrategyPojoStandard(
					bootDescriptor,
					runtimeDescriptor,
					creationContext
			);
		}
	}

}
