/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal.lock;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.persister.entity.internal.ReactiveEntityPersister;
import org.hibernate.sql.exec.internal.lock.DirectLockingCompletion;
import org.hibernate.sql.exec.spi.LoadedValuesCollector;

import static org.hibernate.reactive.util.internal.CompletionStages.failedFuture;
import static org.hibernate.reactive.util.internal.CompletionStages.loop;
import static org.hibernate.reactive.util.internal.CompletionStages.supplyStage;
import static org.hibernate.reactive.util.internal.CompletionStages.voidFuture;

/**
 * Reactive version of {@link DirectLockingCompletion}: the pessimistic force increments
 * of the entities loaded with {@link LockMode#PESSIMISTIC_FORCE_INCREMENT} and
 * {@link jakarta.persistence.PessimisticLockScope#EXTENDED}, which must happen after
 * the collection tables are locked.
 * <p>
 * {@link DirectLockingCompletion} is final, its pending entities are not accessible, and
 * {@link DirectLockingCompletion#complete} runs the acquisition and the increments with
 * blocking JDBC statements.
 * </p>
 */
public final class ReactiveDirectLockingCompletion {
	private final Map<EntityKey, Object> pending = new LinkedHashMap<>();
	private boolean failed;

	public static ReactiveDirectLockingCompletion from(LoadedValuesCollector collector) {
		return collector instanceof ReactiveLoadedValuesCollector reactive
				? reactive.getReactiveDirectLockingCompletion()
				: null;
	}

	/**
	 * @see DirectLockingCompletion#register(EntityKey, Object, EntityEntry)
	 */
	public void register(EntityKey key, Object entity, EntityEntry entry) {
		checkActive();
		if ( !entry.getPersister().isVersioned() ) {
			throw new HibernateException( "Entity '" + entry.getEntityName()
					+ "' has no version and may not be locked at level " + LockMode.PESSIMISTIC_FORCE_INCREMENT );
		}
		final var previous = pending.putIfAbsent( key, entity );
		if ( previous != null && previous != entity ) {
			throw new HibernateException( "Different instances registered for pessimistic force increment: " + key );
		}
	}

	/**
	 * Complete the whole acquisition before starting any increment.
	 *
	 * @see DirectLockingCompletion#complete(Runnable, SharedSessionContractImplementor)
	 */
	public CompletionStage<Void> complete(Supplier<CompletionStage<Void>> acquisition, SharedSessionContractImplementor session) {
		try {
			checkActive();
			pending.forEach( (key, entity) -> managedEntry( key, entity, session ) );
		}
		catch (RuntimeException failure) {
			fail();
			return failedFuture( failure );
		}
		return supplyStage( acquisition )
				.thenCompose( v -> loop( pending.entrySet(), pendingEntity -> {
					final var key = pendingEntity.getKey();
					final var entity = pendingEntity.getValue();
					final var entry = managedEntry( key, entity, session );
					return entry.getLockMode().lessThan( LockMode.PESSIMISTIC_FORCE_INCREMENT )
							? forceVersionIncrement( entity, entry, session )
							: voidFuture();
				} ) )
				.whenComplete( (v, failure) -> {
					if ( failure == null ) {
						pending.clear();
					}
					else {
						fail();
					}
				} );
	}

	/**
	 * The reactive version of {@link org.hibernate.internal.OptimisticLockHelper#forceVersionIncrement},
	 * the same used by the other force increments in Hibernate Reactive.
	 */
	static CompletionStage<Void> forceVersionIncrement(Object entity, EntityEntry entry, SharedSessionContractImplementor session) {
		return ( (ReactiveEntityPersister) entry.getPersister() ).reactiveLock(
				entry.getId(),
				entry.getVersion(),
				entity,
				new LockOptions( LockMode.PESSIMISTIC_FORCE_INCREMENT ),
				session
		);
	}

	public void fail() {
		failed = true;
		pending.clear();
	}

	public void checkActive() {
		if ( failed ) {
			throw new HibernateException( "Cannot resume failed direct-locking completion" );
		}
	}

	private static EntityEntry managedEntry(EntityKey key, Object entity, SharedSessionContractImplementor session) {
		final var context = session.getPersistenceContextInternal();
		final var entry = context.getEntry( entity );
		if ( entry == null || context.getEntity( key ) != entity ) {
			throw new HibernateException( "Cannot complete pessimistic force increment for " + key
					+ ": the original instance is no longer managed" );
		}
		return entry;
	}
}
