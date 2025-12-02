/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.metamodel.internal;

import org.hibernate.bytecode.spi.ReflectionOptimizer;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.metamodel.internal.EntityInstantiatorPojoOptimized;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.bythecode.enhance.spi.internal.ReactiveLazyAttributeLoadingInterceptor;
import org.hibernate.type.descriptor.java.JavaType;

import static org.hibernate.engine.internal.ManagedTypeHelper.asPersistentAttributeInterceptable;

/**
 * Extends {@link EntityInstantiatorPojoOptimized} to apply a {@link ReactiveLazyAttributeLoadingInterceptor}
 */
public class ReactiveEntityInstantiatorPojoOptimized extends EntityInstantiatorPojoOptimized {

	public ReactiveEntityInstantiatorPojoOptimized(
			EntityPersister persister,
			PersistentClass persistentClass,
			JavaType<?> javaType,
			ReflectionOptimizer.InstantiationOptimizer instantiationOptimizer) {
		super( persister, persistentClass, javaType, instantiationOptimizer );
	}

	@Override
	protected Object applyInterception(Object entity) {
		if ( isApplyBytecodeInterception() ) {
			asPersistentAttributeInterceptable( entity )
					.$$_hibernate_setInterceptor( new ReactiveLazyAttributeLoadingInterceptor(
							getLoadingInterceptorState(),
							null,
							null
					) );
		}
		return entity;
	}
}
