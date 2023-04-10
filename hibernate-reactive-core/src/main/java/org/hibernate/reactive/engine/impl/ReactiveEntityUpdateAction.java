/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import java.util.concurrent.CompletionStage;

import org.hibernate.AssertionFailure;
import org.hibernate.CacheMode;
import org.hibernate.HibernateException;
import org.hibernate.action.internal.EntityUpdateAction;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.entry.CacheEntry;
import org.hibernate.engine.spi.CachedNaturalIdValueSource;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.Status;
import org.hibernate.event.spi.EventSource;
import org.hibernate.metamodel.mapping.NaturalIdMapping;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.engine.ReactiveExecutable;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.stat.internal.StatsHelper;
import org.hibernate.stat.spi.StatisticsImplementor;
import org.hibernate.tuple.entity.EntityMetamodel;
import org.hibernate.type.TypeHelper;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * A reactific {@link EntityUpdateAction}.
 */
public class ReactiveEntityUpdateAction extends EntityUpdateAction implements ReactiveExecutable {

	/**
	 * Constructs an EntityUpdateAction
	 *
	 * @param id The entity identifier
	 * @param state The current (extracted) entity state
	 * @param dirtyProperties The indexes (in reference to state) properties with dirty state
	 * @param hasDirtyCollection Were any collections dirty?
	 * @param previousState The previous (stored) state
	 * @param previousVersion The previous (stored) version
	 * @param nextVersion The incremented version
	 * @param instance The entity instance
	 * @param rowId The entity's rowid
	 * @param persister The entity's persister
	 * @param session The session
	 */
	public ReactiveEntityUpdateAction(
			final Object id,
			final Object[] state,
			final int[] dirtyProperties,
			final boolean hasDirtyCollection,
			final Object[] previousState,
			final Object previousVersion,
			final Object nextVersion,
			final Object instance,
			final Object rowId,
			final EntityPersister persister,
			final EventSource session) {
		super( id, state, dirtyProperties, hasDirtyCollection, previousState, previousVersion, nextVersion,
				instance, rowId, persister, session );
	}

	@Override
	public CompletionStage<Void> reactiveExecute() throws HibernateException {
		if ( preUpdate() ) {
			return voidFuture();
		}

		final Object id = getId();
		final EntityPersister persister = getPersister();
		final SharedSessionContractImplementor session = getSession();
		final Object instance = getInstance();
		final Object previousVersion = getPreviousVersion();
		final Object ck = lockCacheItem( previousVersion );

		final ReactiveEntityPersister reactivePersister = (ReactiveEntityPersister) persister;
		return reactivePersister
				.updateReactive(
						id,
						getState(),
						getDirtyFields(),
						hasDirtyCollection(),
						getPreviousState(),
						previousVersion,
						instance,
						getRowId(),
						session
				)
				.thenApply( res -> {
					final EntityEntry entry = session.getPersistenceContextInternal().getEntry( instance );
					if ( entry == null ) {
						throw new AssertionFailure( "possible non-threadsafe access to session" );
					}
					return entry;
				} )
				.thenCompose( this::handleGeneratedProperties )
				.thenAccept( entry -> {
					handleDeleted( entry, persister, instance );
					updateCacheItem( persister, ck, entry );
					handleNaturalIdResolutions( persister, session, id );
					postUpdate();

					final StatisticsImplementor statistics = session.getFactory().getStatistics();
					if ( statistics.isStatisticsEnabled() ) {
						statistics.updateEntity( getPersister().getEntityName() );
					}
				} );
	}

	private CompletionStage<EntityEntry> handleGeneratedProperties(EntityEntry entry) {
		final EntityPersister persister = getPersister();
		if ( entry.getStatus() == Status.MANAGED || persister.isVersionPropertyGenerated() ) {
			final SharedSessionContractImplementor session = getSession();
			final Object instance = getInstance();
			final Object id = getId();
			// get the updated snapshot of the entity state by cloning current state;
			// it is safe to copy in place, since by this time no-one else (should have)
			// has a reference  to the array
			TypeHelper.deepCopy(
					getState(),
					persister.getPropertyTypes(),
					persister.getPropertyCheckability(),
					getState(),
					session
			);
			final ReactiveEntityPersister reactivePersister = (ReactiveEntityPersister) persister;
			return processGeneratedProperties( id, reactivePersister, session, instance )
					// have the entity entry doAfterTransactionCompletion post-update processing, passing it the
					// update state and the new version (if one).
					.thenAccept( v -> entry.postUpdate( instance, getState(), getNextVersion() ) )
					.thenApply( v -> entry );
		}
		else {
			return completedFuture( entry );
		}
	}

	// TODO: copy/paste from superclass (make it protected)
	private void handleDeleted(EntityEntry entry, EntityPersister persister, Object instance) {
		if ( entry.getStatus() == Status.DELETED ) {
			final EntityMetamodel entityMetamodel = persister.getEntityMetamodel();
			final boolean isImpliedOptimisticLocking = !entityMetamodel.isVersioned()
					&& entityMetamodel.getOptimisticLockStyle().isAllOrDirty();
			if ( isImpliedOptimisticLocking && entry.getLoadedState() != null ) {
				// The entity will be deleted and because we are going to create a delete statement
				// that uses all the state values in the where clause, the entry state needs to be
				// updated otherwise the statement execution will not delete any row (see HHH-15218).
				entry.postUpdate(instance, getState(), getNextVersion() );
			}
		}
	}

	// TODO: copy/paste from superclass (make it protected)
	private void handleNaturalIdResolutions(EntityPersister persister, SharedSessionContractImplementor session, Object id) {
		NaturalIdMapping naturalIdMapping = getNaturalIdMapping();
		if ( naturalIdMapping != null ) {
			session.getPersistenceContextInternal().getNaturalIdResolutions().manageSharedResolution(
					id,
					naturalIdMapping.extractNaturalIdFromEntityState( getState() ),
					getPreviousNaturalIdValues(),
					persister,
					CachedNaturalIdValueSource.UPDATE
			);
		}
	}

	// TODO: copy/paste from superclass (make it protected)
	private void updateCacheItem(Object previousVersion, Object ck, EntityEntry entry) {
		final EntityPersister persister = getPersister();
		if ( persister.canWriteToCache() ) {
			final SharedSessionContractImplementor session = getSession();
			if ( isCacheInvalidationRequired( persister, session ) || entry.getStatus() != Status.MANAGED ) {
				persister.getCacheAccessStrategy().remove( session, ck );
			}
			else if ( session.getCacheMode().isPutEnabled() ) {
				//TODO: inefficient if that cache is just going to ignore the updated state!
				final CacheEntry ce = persister.buildCacheEntry( getInstance(), getState(), getNextVersion(), getSession() );
				setCacheEntry( persister.getCacheEntryStructure().structure( ce ) );
				final boolean put = updateCache( persister, previousVersion, ck );

				final StatisticsImplementor statistics = session.getFactory().getStatistics();
				if ( put && statistics.isStatisticsEnabled() ) {
					statistics.entityCachePut(
							StatsHelper.INSTANCE.getRootEntityRole(persister),
							getPersister().getCacheAccessStrategy().getRegion().getName()
					);
				}
			}
		}
	}

	private static boolean isCacheInvalidationRequired(
			EntityPersister persister,
			SharedSessionContractImplementor session) {
		// the cache has to be invalidated when CacheMode is equal to GET or IGNORE
		return persister.isCacheInvalidationRequired()
			|| session.getCacheMode() == CacheMode.GET
			|| session.getCacheMode() == CacheMode.IGNORE;
	}

	// TODO: copy/paste from superclass (make it protected)
	private Object lockCacheItem(Object previousVersion) {
		final EntityPersister persister = getPersister();
		if ( persister.canWriteToCache() ) {
			final SharedSessionContractImplementor session = getSession();
			final EntityDataAccess cache = persister.getCacheAccessStrategy();
			final Object ck = cache.generateCacheKey(
					getId(),
					persister,
					session.getFactory(),
					session.getTenantIdentifier()
			);
			setLock( cache.lockItem( session, ck, previousVersion ) );
			return ck;
		}
		else {
			return null;
		}
	}

	private CompletionStage<Void> processGeneratedProperties(
			Object id,
			ReactiveEntityPersister persister,
			SharedSessionContractImplementor session,
			Object instance) {
		if ( persister.hasUpdateGeneratedProperties() ) {
			// this entity defines property generation, so process those generated
			// values...
			if ( persister.isVersionPropertyGenerated() ) {
				throw new UnsupportedOperationException( "generated version attribute not supported in Hibernate Reactive" );
//				setNextVersion( Versioning.getVersion( getState(), persister ) );
			}
			return persister.reactiveProcessUpdateGenerated( id, instance, getState(), session );

		}
		else {
			return voidFuture();
		}
	}

	@Override
	public void execute() throws HibernateException {
		throw new UnsupportedOperationException( "This action only support reactive functions calls" );
	}
}
