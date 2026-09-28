/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.context.internal;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.hibernate.reactive.context.Context;

import io.vertx.core.internal.VertxBootstrap;
import io.vertx.core.spi.VertxServiceProvider;
import io.vertx.core.spi.context.storage.ContextLocal;

import static io.vertx.core.spi.context.storage.ContextLocal.registerLocal;

/**
 * SPI Implementation for {@link ContextLocal} storage.
 */
public class ContextualDataStorage implements VertxServiceProvider {

	private static final ContextLocal<ConcurrentHashMap> CONTEXTUAL_DATA_KEY =
			registerLocal( ConcurrentHashMap.class, ConcurrentHashMap::new );

	@Override
	public void init(VertxBootstrap builder) {
	}

	@SuppressWarnings("unchecked")
	public static <T> void put(io.vertx.core.Context vertxContext, Context.Key<T> key, T value) {
		( (Map<Context.Key<T>, T>) vertxContext
				.getLocal( CONTEXTUAL_DATA_KEY, ConcurrentHashMap::new ) )
				.put( key, value );
	}

	@SuppressWarnings("unchecked")
	public static <T> T get(io.vertx.core.Context vertxContext, Context.Key<T> key) {
		Map<Context.Key<T>, T> map = vertxContext.getLocal( CONTEXTUAL_DATA_KEY );
		return map == null ? null : map.get( key );
	}

	public static boolean remove(io.vertx.core.Context vertxContext, Context.Key<?> key) {
		var map = vertxContext.getLocal( CONTEXTUAL_DATA_KEY );
		return map != null && map.remove( key ) != null;
	}
}
