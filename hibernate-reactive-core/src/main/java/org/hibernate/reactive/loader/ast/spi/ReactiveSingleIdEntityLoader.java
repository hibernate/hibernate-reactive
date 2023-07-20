/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.spi;

import org.hibernate.reactive.engine.impl.InternalStage;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.ast.spi.SingleIdEntityLoader;
import org.hibernate.reactive.logging.impl.Log;

import static java.lang.invoke.MethodHandles.lookup;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;

/**
 * Reactive version of {@link SingleIdEntityLoader}.
 * @param <T> the entity class
 */
public interface ReactiveSingleIdEntityLoader<T> extends SingleIdEntityLoader<InternalStage<T>> {

	/**
	 * @deprecated use {@link #reactiveLoadDatabaseSnapshot(Object, SharedSessionContractImplementor)}
	 */
	@Deprecated
	@Override
	default Object[] loadDatabaseSnapshot(Object id, SharedSessionContractImplementor session) {
		throw make( Log.class, lookup() ).nonReactiveMethodCall( "reactiveLoadDatabaseSnapshot" );
	}

	InternalStage<Object[]> reactiveLoadDatabaseSnapshot(Object id, SharedSessionContractImplementor session);
}
