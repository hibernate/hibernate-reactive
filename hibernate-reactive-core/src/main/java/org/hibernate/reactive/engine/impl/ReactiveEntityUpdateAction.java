/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import java.util.concurrent.CompletionStage;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.action.internal.EntityUpdateAction;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.entry.CacheEntry;
import org.hibernate.engine.spi.CachedNaturalIdValueSource;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.Status;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.engine.ReactiveExecutable;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.stat.internal.StatsHelper;
import org.hibernate.stat.spi.StatisticsImplementor;
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
			final SharedSessionContractImplementor session) {
		super( id, state, dirtyProperties, hasDirtyCollection, previousState, previousVersion, nextVersion,
				instance, rowId, persister, session );
	}

	@Override
	public CompletionStage<Void> reactiveExecute() throws HibernateException {
		final Object id = getId();
		final EntityPersister persister = getPersister();
		final SharedSessionContractImplementor session = getSession();
		final Object instance = getInstance();

		if ( preUpdate() ) {
			return voidFuture();
		}

		final SessionFactoryImplementor factory = session.getFactory();
		final Object previousVersion = persister.isVersionPropertyGenerated()
				// we need to grab the version value from the entity, otherwise
				// we have issues with generated-version entities that may have
				// multiple actions queued during the same flush
				? persister.getVersion( instance )
				: getPreviousVersion();

		final Object ck;
		if ( persister.canWriteToCache() ) {
			final EntityDataAccess cache = persister.getCacheAccessStrategy();
			ck = cache.generateCacheKey(
					id,
					persister,
					factory,
					session.getTenantIdentifier()
			);
			setLock( cache.lockItem( session, ck, previousVersion ) );
		}
		else {
			ck = null;
		}

		ReactiveEntityPersister reactivePersister = (ReactiveEntityPersister) persister;
		return reactivePersister
				.updateReactive( id, getState(), getDirtyFields(), hasDirtyCollection(), getPreviousState(), previousVersion, instance, getRowId(), session )
				.thenApply( res -> {
					final EntityEntry entry = session.getPersistenceContextInternal().getEntry( instance );
					if ( entry == null ) {
						throw new AssertionFailure( "possible non-threadsafe access to session" );
					}
					return entry;
				} )
				.thenCompose( entry -> {
					if ( entry.getStatus() == Status.MANAGED || persister.isVersionPropertyGenerated() ) {
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
						return processGeneratedProperties( id, reactivePersister, session, instance )
								// have the entity entry doAfterTransactionCompletion post-update processing, passing it the
								// update state and the new version (if one).
								.thenAccept( v -> entry.postUpdate( instance, getState(), getNextVersion() ) )
								.thenApply( v -> entry );
					}
					return completedFuture( entry );
				} )
				.thenAccept( entry -> {
					final StatisticsImplementor statistics = factory.getStatistics();
					if ( persister.canWriteToCache() ) {
						if ( persister.isCacheInvalidationRequired() || entry.getStatus() != Status.MANAGED ) {
							persister.getCacheAccessStrategy().remove( session, ck );
						}
						else if ( session.getCacheMode().isPutEnabled() ) {
							//TODO: inefficient if that cache is just going to ignore the updated state!
							final CacheEntry ce = persister.buildCacheEntry( instance, getState(), getNextVersion(), getSession() );
								setCacheEntry( persister.getCacheEntryStructure().structure( ce ) );

							final boolean put = updateCache( persister, previousVersion, ck );
							if ( put && statistics.isStatisticsEnabled() ) {
								statistics.entityCachePut(
										StatsHelper.INSTANCE.getRootEntityRole( persister ),
										getPersister().getCacheAccessStrategy().getRegion().getName()
								);
							}
						}
					}

					if ( getNaturalIdMapping() != null ) {
						session.getPersistenceContextInternal().getNaturalIdResolutions().manageSharedResolution(
								id,
								getNaturalIdMapping().extractNaturalIdFromEntityState( getState(), session ),
								getPreviousNaturalIdValues(),
								persister,
								CachedNaturalIdValueSource.UPDATE
						);
					}

					postUpdate();

					if ( statistics.isStatisticsEnabled() ) {
						statistics.updateEntity( getPersister().getEntityName() );
					}
				} );
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
		return voidFuture();
	}

	@Override
	public void execute() throws HibernateException {
		throw new UnsupportedOperationException( "This action only support reactive functions calls" );
	}
}
