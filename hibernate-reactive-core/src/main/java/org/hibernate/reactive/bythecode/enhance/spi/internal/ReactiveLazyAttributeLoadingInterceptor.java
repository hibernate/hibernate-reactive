/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.bythecode.enhance.spi.internal;

import org.hibernate.bytecode.enhance.spi.interceptor.LazyAttributeLoadingInterceptor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;

import java.lang.invoke.MethodHandles;

/**
 * Reactive version of {@link LazyAttributeLoadingInterceptor}.
 *
 * It throws a {@link org.hibernate.LazyInitializationException} when a lazy attribute
 * is not fetched using {@link org.hibernate.reactive.mutiny.Mutiny#fetch(Object)}
 * or {@link org.hibernate.reactive.stage.Stage#fetch(Object)} but transparently
 */
public class ReactiveLazyAttributeLoadingInterceptor extends LazyAttributeLoadingInterceptor {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveLazyAttributeLoadingInterceptor(
			EntityRelatedState entityMeta,
			Object identifier,
			SharedSessionContractImplementor session) {
		super( entityMeta, identifier, session );
	}

	@Override
	protected Object handleRead(Object target, String attributeName, Object value) {
		if ( !isAttributeLoaded( attributeName ) ) {
			throw LOG.lazyFieldInitializationException( attributeName, getEntityName() );
		}
		return value;
	}
}
