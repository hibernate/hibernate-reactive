/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.metamodel.internal;

import org.hibernate.mapping.PersistentClass;
import org.hibernate.metamodel.internal.EntityRepresentationStrategyPojoStandard;
import org.hibernate.metamodel.spi.EntityInstantiator;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.persister.entity.EntityPersister;

/**
 * Extends {@link EntityRepresentationStrategyPojoStandard}
 * to create a {@link ReactiveEntityInstantiatorPojoStandard}
 */
public class ReactiveEntityRepresentationStrategyPojoStandard extends EntityRepresentationStrategyPojoStandard {

	public ReactiveEntityRepresentationStrategyPojoStandard(
			PersistentClass bootDescriptor,
			EntityPersister runtimeDescriptor,
			RuntimeModelCreationContext creationContext) {
		super( bootDescriptor, runtimeDescriptor, creationContext );
	}

	@Override
	protected EntityInstantiator determineInstantiator(PersistentClass bootDescriptor, EntityPersister persister) {
		return new ReactiveEntityInstantiatorPojoStandard(
				persister,
				bootDescriptor,
				getMappedJavaType(),
				super.determineInstantiator( bootDescriptor, persister )
		);
	}
}
