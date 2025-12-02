/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.metamodel.internal;

import org.hibernate.mapping.PersistentClass;
import org.hibernate.metamodel.internal.EntityInstantiatorPojoStandard;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.bythecode.enhance.spi.internal.ReactiveLazyAttributeLoadingInterceptor;
import org.hibernate.type.descriptor.java.JavaType;

import static org.hibernate.engine.internal.ManagedTypeHelper.asPersistentAttributeInterceptable;

/**
 * Extends {@link EntityInstantiatorPojoStandard} to apply a {@link ReactiveLazyAttributeLoadingInterceptor}
 */
public class ReactiveEntityInstantiatorPojoStandard extends EntityInstantiatorPojoStandard {

	public ReactiveEntityInstantiatorPojoStandard(
			EntityPersister persister,
			PersistentClass persistentClass,
			JavaType<?> javaType) {
		super( persister, persistentClass, javaType );
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
