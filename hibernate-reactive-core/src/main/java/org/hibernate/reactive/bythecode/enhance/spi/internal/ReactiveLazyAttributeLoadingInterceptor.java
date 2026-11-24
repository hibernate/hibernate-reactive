/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.bythecode.enhance.spi.internal;

import org.hibernate.bytecode.enhance.spi.interceptor.LazyAttributeLoadingInterceptor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.logging.internal.Log;
import org.hibernate.reactive.logging.internal.LoggerFactory;

import java.lang.invoke.MethodHandles;

/**
 * Reactive version of {@link LazyAttributeLoadingInterceptor}.
 *
 * It throws a {@link org.hibernate.LazyInitializationException} when the application reads
 * a lazy attribute that it didn't fetch with {@link org.hibernate.reactive.mutiny.Mutiny#fetch(Object)}
 * or {@link org.hibernate.reactive.stage.Stage#fetch(Object)}.
 */
public class ReactiveLazyAttributeLoadingInterceptor extends LazyAttributeLoadingInterceptor {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveLazyAttributeLoadingInterceptor(
			EntityRelatedState entityMeta,
			Object identifier,
			SharedSessionContractImplementor session) {
		super( entityMeta, identifier, session );
	}

	/**
	 * Hibernate ORM loads an attribute that is not loaded yet when the application reads it,
	 * with a blocking query. A reactive session can't run it: there is no JDBC connection,
	 * and a getter can't return a {@link java.util.concurrent.CompletionStage}.
	 * So we fail with an error that tells the application to fetch the attribute explicitly,
	 * instead of a less clear error from inside the blocking load.
	 */
	@Override
	protected Object handleRead(Object target, String attributeName, Object value) {
		if ( !isAttributeLoaded( attributeName ) ) {
			throw LOG.lazyFieldInitializationException( attributeName, getEntityName() );
		}
		return value;
	}
}
