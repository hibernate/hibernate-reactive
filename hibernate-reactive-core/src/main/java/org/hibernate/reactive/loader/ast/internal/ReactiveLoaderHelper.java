/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.lang.reflect.Array;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.Hibernate;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.ObjectDeletedException;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.SubselectFetch;
import org.hibernate.event.monitor.spi.DiagnosticEvent;
import org.hibernate.event.monitor.spi.EventMonitor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.internal.OptimisticLockHelper;
import org.hibernate.loader.LoaderLogging;
import org.hibernate.metamodel.mapping.JdbcMapping;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveSelectExecutor;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.internal.JdbcOperationQuerySelect;
import org.hibernate.sql.exec.internal.JdbcParameterBindingImpl;
import org.hibernate.sql.exec.internal.JdbcParameterBindingsImpl;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.exec.spi.JdbcParametersList;
import org.hibernate.sql.results.internal.RowTransformerStandardImpl;

import static java.util.Objects.requireNonNull;
import static org.hibernate.reactive.util.impl.CompletionStages.supplyStage;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * @see org.hibernate.loader.ast.internal.LoaderHelper
 */
public class ReactiveLoaderHelper {

	/**
	 * Creates a typed array, as opposed to a generic {@code Object[]} that holds the typed values
	 *
	 * @param elementClass The type of the array elements.  See {@link Class#getComponentType()}
	 * @param length The length to which the array should be created.  This is usually zero for Hibernate uses
	 */
	public static <X> X[] createTypedArray(Class<X> elementClass, @SuppressWarnings("SameParameterValue") int length) {
		//noinspection unchecked
		return (X[]) Array.newInstance( elementClass, length );
	}

	/**
	 * Load one or more instances of a model part (an entity or collection)
	 * based on a SQL ARRAY parameter to specify the keys (as opposed to the
	 * more traditional SQL IN predicate approach).
	 *
	 * @param <R> The type of the model part to load
	 * @param <K> The type of the keys
	 */
	public static <R, K> CompletionStage<List<R>> loadByArrayParameter(
			K[] idsToInitialize,
			SelectStatement sqlAst,
			JdbcOperationQuerySelect jdbcOperation,
			JdbcParameter jdbcParameter,
			JdbcMapping arrayJdbcMapping,
			Object entityId,
			Object entityInstance,
			LockOptions lockOptions,
			Boolean readOnly,
			SharedSessionContractImplementor session) {
		requireNonNull( jdbcOperation );
		requireNonNull( jdbcParameter );

		final JdbcParameterBindings jdbcParameterBindings = new JdbcParameterBindingsImpl( 1 );
		jdbcParameterBindings.addBinding(
				jdbcParameter,
				new JdbcParameterBindingImpl( arrayJdbcMapping, idsToInitialize )
		);

		final SubselectFetch.RegistrationHandler subSelectFetchableKeysHandler = SubselectFetch.createRegistrationHandler(
				session.getPersistenceContext().getBatchFetchQueue(),
				sqlAst,
				JdbcParametersList.singleton( jdbcParameter ),
				jdbcParameterBindings
		);

		return StandardReactiveSelectExecutor.INSTANCE.list(
				jdbcOperation,
				jdbcParameterBindings,
				new SingleIdExecutionContext(
						entityId,
						entityInstance,
						readOnly,
						lockOptions,
						subSelectFetchableKeysHandler,
						session
				),
				RowTransformerStandardImpl.instance(),
				ReactiveListResultsConsumer.UniqueSemantic.FILTER
		);
	}

	/**
	 * A Reactive implementation of {@link org.hibernate.loader.ast.internal.LoaderHelper#upgradeLock(Object, EntityEntry, LockOptions, SharedSessionContractImplementor)}
	 */
	public static CompletionStage<Void> upgradeLock(
			Object object,
			EntityEntry entry,
			LockOptions lockOptions,
			SharedSessionContractImplementor session) {
		final LockMode requestedLockMode = lockOptions.getLockMode();
		if ( requestedLockMode.greaterThan( entry.getLockMode() ) ) {
			// Request is for a more restrictive lock than the lock already held
			final ReactiveEntityPersister persister = (ReactiveEntityPersister) entry.getPersister();

			if ( entry.getStatus().isDeletedOrGone()) {
				throw new ObjectDeletedException(
						"attempted to lock a deleted instance",
						entry.getId(),
						persister.getEntityName()
				);
			}

			if ( LoaderLogging.LOADER_LOGGER.isTraceEnabled() ) {
				LoaderLogging.LOADER_LOGGER.tracef(
						"Locking `%s( %s )` in `%s` lock-mode",
						persister.getEntityName(),
						entry.getId(),
						requestedLockMode
				);
			}

			final boolean cachingEnabled = persister.canWriteToCache();
			SoftLock lock = null;
			Object ck = null;
			try {
				if ( cachingEnabled ) {
					final EntityDataAccess cache = persister.getCacheAccessStrategy();
					ck = cache.generateCacheKey( entry.getId(), persister, session.getFactory(), session.getTenantIdentifier() );
					lock = cache.lockItem( session, ck, entry.getVersion() );
				}

				if ( persister.isVersioned() && entry.getVersion() == null ) {
					// This should be an empty entry created for an uninitialized bytecode proxy
					if ( !Hibernate.isPropertyInitialized( object, persister.getVersionMapping().getPartName() ) ) {
						Hibernate.initialize( object );
						entry = session.getPersistenceContextInternal().getEntry( object );
						assert entry.getVersion() != null;
					}
					else {
						throw new IllegalStateException( String.format(
								"Trying to lock versioned entity %s but found null version",
								MessageHelper.infoString( persister.getEntityName(), entry.getId() )
						) );
					}
				}

				if ( persister.isVersioned() && requestedLockMode == LockMode.PESSIMISTIC_FORCE_INCREMENT  ) {
					// todo : should we check the current isolation mode explicitly?
					OptimisticLockHelper.forceVersionIncrement( object, entry, session );
				}
				else if ( entry.isExistsInDatabase() ) {
					final EventMonitor eventMonitor = session.getEventMonitor();
					final DiagnosticEvent entityLockEvent = eventMonitor.beginEntityLockEvent();
					return reactiveLock( object, entry, lockOptions, session, persister, eventMonitor, entityLockEvent, cachingEnabled, ck, lock );
				}
				else {
					// should only be possible for a stateful session
					if ( session instanceof EventSource eventSource ) {
						eventSource.forceFlush( entry );
					}
				}
				entry.setLockMode(requestedLockMode);
			}
			finally {
				// the database now holds a lock + the object is flushed from the cache,
				// so release the soft lock
				if ( cachingEnabled ) {
					persister.getCacheAccessStrategy().unlockItem( session, ck, lock );
				}
			}
		}
		return voidFuture();
	}

	private static CompletionStage<Void> reactiveLock(
			Object object,
			EntityEntry entry,
			LockOptions lockOptions,
			SharedSessionContractImplementor session,
			ReactiveEntityPersister persister,
			EventMonitor eventMonitor,
			DiagnosticEvent entityLockEvent,
			boolean cachingEnabled,
			Object ck,
			SoftLock lock) {
		return supplyStage( () -> supplyStage( () ->  persister.reactiveLock( entry.getId(), entry.getVersion(), object, lockOptions, session ) )
				.whenComplete( (v, e) -> completeLockEvent( entry, lockOptions, session, persister, eventMonitor, entityLockEvent, cachingEnabled, ck, lock, e == null ) ) )
				.whenComplete( (v, e) -> {
					if ( cachingEnabled ) {
						persister.getCacheAccessStrategy().unlockItem( session, ck, lock );
					}
				} );
	}

	private static void completeLockEvent(
			EntityEntry entry,
			LockOptions lockOptions,
			SharedSessionContractImplementor session,
			ReactiveEntityPersister persister,
			EventMonitor eventMonitor,
			DiagnosticEvent entityLockEvent,
			boolean cachingEnabled,
			Object ck,
			SoftLock lock,
			boolean succes) {
		eventMonitor.completeEntityLockEvent(
				entityLockEvent,
				entry.getId(),
				persister.getEntityName(),
				lockOptions.getLockMode(),
				succes,
				session
		);
		if ( cachingEnabled ) {
			persister.getCacheAccessStrategy().unlockItem( session, ck, lock );
		}
	}

}
