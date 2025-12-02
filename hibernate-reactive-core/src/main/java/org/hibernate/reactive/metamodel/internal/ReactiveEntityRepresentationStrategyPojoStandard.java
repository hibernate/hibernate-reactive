/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.metamodel.internal;

import org.hibernate.bytecode.spi.ReflectionOptimizer;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.metamodel.internal.EntityRepresentationStrategyPojoStandard;
import org.hibernate.metamodel.spi.EntityInstantiator;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.tuple.entity.EntityMetamodel;

/**
 * Extends {@link EntityRepresentationStrategyPojoStandard}
 * to create {@link ReactiveEntityInstantiatorPojoOptimized} and {@link ReactiveEntityInstantiatorPojoStandard}
 */
public class ReactiveEntityRepresentationStrategyPojoStandard extends EntityRepresentationStrategyPojoStandard {

	public ReactiveEntityRepresentationStrategyPojoStandard(
			PersistentClass bootDescriptor,
			EntityPersister runtimeDescriptor,
			RuntimeModelCreationContext creationContext) {
		super( bootDescriptor, runtimeDescriptor, creationContext );
	}

	@Override
	protected EntityInstantiator determineInstantiator(
			PersistentClass bootDescriptor,
			EntityMetamodel entityMetamodel) {
		final ReflectionOptimizer reflectionOptimizer = getReflectionOptimizer();
		if ( reflectionOptimizer != null && reflectionOptimizer.getInstantiationOptimizer() != null ) {
			return new ReactiveEntityInstantiatorPojoOptimized(
					entityMetamodel,
					bootDescriptor,
					getMappedJavaType(),
					reflectionOptimizer.getInstantiationOptimizer()
			);
		}
		else {
			return new ReactiveEntityInstantiatorPojoStandard( entityMetamodel, bootDescriptor, getMappedJavaType() );
		}
	}

}
