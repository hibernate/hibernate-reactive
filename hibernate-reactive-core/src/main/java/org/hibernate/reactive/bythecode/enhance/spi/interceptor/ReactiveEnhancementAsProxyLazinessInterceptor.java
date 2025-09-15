/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.bythecode.enhance.spi.interceptor;

import org.hibernate.bytecode.enhance.spi.interceptor.EnhancementAsProxyLazinessInterceptor;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;

import java.lang.invoke.MethodHandles;

/**
 * Reactive version of {@link EnhancementAsProxyLazinessInterceptor}.
 *
 * It throws a {@link org.hibernate.LazyInitializationException} when a lazy attribute
 * is not fetched using {@link org.hibernate.reactive.mutiny.Mutiny#fetch(Object)}
 * or {@link org.hibernate.reactive.stage.Stage#fetch(Object)} but transparently
 */
public class ReactiveEnhancementAsProxyLazinessInterceptor extends EnhancementAsProxyLazinessInterceptor {
	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveEnhancementAsProxyLazinessInterceptor(
			EntityRelatedState meta,
			EntityKey entityKey,
			SharedSessionContractImplementor session) {
		super( meta, entityKey, session );
	}

	@Override
	protected Object handleRead(Object target, String attributeName, Object value) {
		if ( isIdentifier( attributeName ) ) {
			return super.handleRead( target, attributeName, value );
		}
		else {
			throw LOG.lazyFieldInitializationException( attributeName, getEntityName() );
		}
	}
}
