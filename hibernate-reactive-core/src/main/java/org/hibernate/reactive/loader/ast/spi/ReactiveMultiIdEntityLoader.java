/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.spi;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.event.spi.EventSource;
import org.hibernate.loader.ast.spi.MultiIdEntityLoader;
import org.hibernate.loader.ast.spi.MultiIdLoadOptions;
import org.hibernate.reactive.logging.impl.Log;

import static java.lang.invoke.MethodHandles.lookup;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;

/**
 * @see org.hibernate.loader.ast.spi.MultiIdEntityLoader
 */
public interface ReactiveMultiIdEntityLoader<T> extends MultiIdEntityLoader<T> {

	@Override
	default <K> List<T> load(K[] ids, MultiIdLoadOptions options, EventSource session) {
		throw make( Log.class, lookup() ).nonReactiveMethodCall( "reactiveLoad" );
	}

	<K> CompletionStage<List<T>> reactiveLoad(K[] ids, MultiIdLoadOptions options, EventSource session);
}
