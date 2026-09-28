/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.metamodel.internal;

import org.hibernate.mapping.PersistentClass;
import org.hibernate.metamodel.internal.AbstractEntityInstantiatorPojo;
import org.hibernate.metamodel.spi.EntityInstantiator;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.bythecode.enhance.spi.internal.ReactiveLazyAttributeLoadingInterceptor;
import org.hibernate.type.descriptor.java.JavaType;

import static org.hibernate.engine.internal.ManagedTypeHelper.asPersistentAttributeInterceptable;

/**
 * Wraps an {@link EntityInstantiator} to apply a {@link ReactiveLazyAttributeLoadingInterceptor}
 */
public class ReactiveEntityInstantiatorPojoOptimized extends AbstractEntityInstantiatorPojo {

	private final EntityInstantiator delegate;

	public ReactiveEntityInstantiatorPojoOptimized(
			EntityPersister persister,
			PersistentClass persistentClass,
			JavaType<?> javaType,
			EntityInstantiator delegate) {
		super( persister, persistentClass, javaType );
		this.delegate = delegate;
	}

	@Override
	public Object instantiate() {
		Object entity = delegate.instantiate();
		return applyInterception( entity );
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
