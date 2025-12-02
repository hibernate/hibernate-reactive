/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.internal;

import org.hibernate.LockOptions;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.spi.LoadEventListener;
import org.hibernate.loader.internal.CacheLoadHelper;
import org.hibernate.loader.internal.CacheLoadHelper.PersistenceContextEntry.EntityStatus;

import java.util.concurrent.CompletionStage;

import static org.hibernate.loader.internal.CacheLoadHelper.PersistenceContextEntry.EntityStatus.MANAGED;
import static org.hibernate.reactive.loader.ast.internal.ReactiveLoaderHelper.upgradeLock;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

/**
 * A reactive implementation of {@link  CacheLoadHelper}
 */
public class ReactiveCacheLoadHelper {

	public static CompletionStage<CacheLoadHelper.PersistenceContextEntry> loadFromSessionCache(
			EntityKey keyToLoad,
			LockOptions lockOptions,
			LoadEventListener.LoadType options,
			SharedSessionContractImplementor session) {
		final Object old = session.getEntityUsingInterceptor( keyToLoad );
		EntityStatus entityStatus = MANAGED;
		if ( old != null ) {
			// this object was already loaded
			final EntityEntry oldEntry = session.getPersistenceContext().getEntry( old );
			entityStatus = CacheLoadHelper.entityStatus( keyToLoad, options, session, oldEntry, old );
			if ( entityStatus == MANAGED ) {
				return upgradeLock( old, oldEntry, lockOptions, session )
						.thenApply(v -> new CacheLoadHelper.PersistenceContextEntry( old, MANAGED ) );
			}
		}
		return completedFuture( new CacheLoadHelper.PersistenceContextEntry( old, entityStatus ) );
	}

}
