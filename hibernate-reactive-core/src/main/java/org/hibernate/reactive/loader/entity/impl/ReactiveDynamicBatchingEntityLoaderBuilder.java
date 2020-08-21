/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.entity.impl;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.Status;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.LoadEvent;
import org.hibernate.event.spi.LoadEventListener;
import org.hibernate.internal.util.collections.CollectionHelper;
import org.hibernate.loader.entity.CacheEntityLoaderHelper;
import org.hibernate.loader.entity.UniqueEntityLoader;
import org.hibernate.persister.entity.MultiLoadOptions;
import org.hibernate.persister.entity.OuterJoinLoadable;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * A {@link ReactiveBatchingEntityLoaderBuilder} that is enabled when
 * {@link org.hibernate.loader.BatchFetchStyle#DYNAMIC} is selected.
 *
 * A factory for {@link ReactiveDynamicBatchingEntityDelegator}s.
 *
 * @see org.hibernate.loader.entity.DynamicBatchingEntityLoaderBuilder
 */
public class ReactiveDynamicBatchingEntityLoaderBuilder extends ReactiveBatchingEntityLoaderBuilder {

	public static final ReactiveDynamicBatchingEntityLoaderBuilder INSTANCE = new ReactiveDynamicBatchingEntityLoaderBuilder();

	public CompletionStage<List<Object>> multiLoad(
			OuterJoinLoadable persister,
			Serializable[] ids,
			SessionImplementor session,
			MultiLoadOptions loadOptions) {
		return loadOptions.isOrderReturnEnabled() ?
				performOrderedMultiLoad(persister, ids, session, loadOptions) :
				performUnorderedMultiLoad(persister, ids, session, loadOptions);
	}

	private CompletionStage<List<Object>> performOrderedBatchLoad(
			List<Serializable> idsInBatch,
			LockOptions lockOptions,
			OuterJoinLoadable persister,
			SessionImplementor session) {
		final int batchSize =  idsInBatch.size();
		final ReactiveDynamicBatchingEntityLoader batchingLoader = new ReactiveDynamicBatchingEntityLoader(
				persister,
				batchSize,
				lockOptions,
				session.getFactory(),
				session.getLoadQueryInfluencers()
		);

		final Serializable[] idsInBatchArray = idsInBatch.toArray(new Serializable[0]);
		QueryParameters qp = buildMultiLoadQueryParameters( persister, idsInBatchArray, lockOptions );
		CompletionStage<List<Object>> result = batchingLoader.doEntityBatchFetch(session, qp, idsInBatchArray);
		idsInBatch.clear();
		return result;
	}

	private CompletionStage<List<Object>> performUnorderedMultiLoad(
			OuterJoinLoadable persister,
			Serializable[] ids,
			SessionImplementor session,
			MultiLoadOptions loadOptions) {
		assert !loadOptions.isOrderReturnEnabled();

		final List<Object> result = CollectionHelper.arrayList( ids.length );

		final LockOptions lockOptions = loadOptions.getLockOptions() == null
				? new LockOptions( LockMode.NONE )
				: loadOptions.getLockOptions();

		if ( loadOptions.isSessionCheckingEnabled() || loadOptions.isSecondLevelCacheCheckingEnabled() ) {
			// the user requested that we exclude ids corresponding to already managed
			// entities from the generated load SQL.  So here we will iterate all
			// incoming id values and see whether it corresponds to an existing
			// entity associated with the PC - if it does we add it to the result
			// list immediately and remove its id from the group of ids to load.
			boolean foundAnyManagedEntities = false;
			final List<Serializable> nonManagedIds = new ArrayList<>();
			for ( Serializable id : ids ) {
				final EntityKey entityKey = new EntityKey( id, persister );

				LoadEvent loadEvent = new LoadEvent(
						id,
						persister.getMappedClass().getName(),
						lockOptions,
						(EventSource) session,
						null
				);

				Object managedEntity = null;

				// look for it in the Session first
				CacheEntityLoaderHelper.PersistenceContextEntry persistenceContextEntry =
						CacheEntityLoaderHelper.INSTANCE.loadFromSessionCache(
								loadEvent,
								entityKey,
								LoadEventListener.GET
						);
				if ( loadOptions.isSessionCheckingEnabled() ) {
					managedEntity = persistenceContextEntry.getEntity();

					if ( managedEntity != null
							&& !loadOptions.isReturnOfDeletedEntitiesEnabled()
							&& !persistenceContextEntry.isManaged() ) {
						foundAnyManagedEntities = true;
						result.add( null );
						continue;
					}
				}

				if ( managedEntity == null && loadOptions.isSecondLevelCacheCheckingEnabled() ) {
					managedEntity = CacheEntityLoaderHelper.INSTANCE.loadFromSecondLevelCache(
							loadEvent,
							persister,
							entityKey
					);
				}

				if ( managedEntity != null ) {
					foundAnyManagedEntities = true;
					result.add( managedEntity );
				}
				else {
					nonManagedIds.add( id );
				}
			}

			if ( foundAnyManagedEntities ) {
				if ( nonManagedIds.isEmpty() ) {
					// all of the given ids were already associated with the Session
					return CompletionStages.completedFuture(result);
				}
				else {
					// over-write the ids to be loaded with the collection of
					// just non-managed ones
					ids = nonManagedIds.toArray(
							(Serializable[]) Array.newInstance(
									ids.getClass().getComponentType(),
									nonManagedIds.size()
							)
					);
				}
			}
		}

		int numberOfIdsLeft = ids.length;
		final int maxBatchSize;
		if ( loadOptions.getBatchSize() != null && loadOptions.getBatchSize() > 0 ) {
			maxBatchSize = loadOptions.getBatchSize();
		}
		else {
			maxBatchSize = session.getJdbcServices().getJdbcEnvironment().getDialect()
					.getDefaultBatchLoadSizingStrategy()
					.determineOptimalBatchLoadSize(
							persister.getIdentifierType().getColumnSpan( session.getFactory() ),
							numberOfIdsLeft
					);
		}

		CompletionStage<Void> stage = CompletionStages.voidFuture();
		//TODO: Trampoline this!
		int idPosition = 0;
		while ( numberOfIdsLeft > 0 ) {
			int batchSize =  Math.min( numberOfIdsLeft, maxBatchSize );
			final ReactiveDynamicBatchingEntityLoader batchingLoader = new ReactiveDynamicBatchingEntityLoader(
					persister,
					batchSize,
					lockOptions,
					session.getFactory(),
					session.getLoadQueryInfluencers()
			);

			Serializable[] idsInBatch = new Serializable[batchSize];
			System.arraycopy( ids, idPosition, idsInBatch, 0, batchSize );

			QueryParameters qp = buildMultiLoadQueryParameters( persister, idsInBatch, lockOptions );
			CompletionStage<Void> fetch =
					batchingLoader.doEntityBatchFetch(session, qp, idsInBatch)
							.thenAccept(result::addAll);
			stage = stage.thenCompose( v -> fetch );

			numberOfIdsLeft = numberOfIdsLeft - batchSize;
			idPosition += batchSize;
		}

		return stage.thenApply( v -> result );
	}

	private static QueryParameters buildMultiLoadQueryParameters(
			OuterJoinLoadable persister,
			Serializable[] ids,
			LockOptions lockOptions) {
		Type[] types = new Type[ids.length];
		Arrays.fill( types, persister.getIdentifierType() );

		QueryParameters qp = new QueryParameters();
		qp.setOptionalEntityName( persister.getEntityName() );
		qp.setPositionalParameterTypes( types );
		qp.setPositionalParameterValues( ids );
		qp.setLockOptions( lockOptions );
		qp.setOptionalObject( null );
		qp.setOptionalId( null );
		return qp;
	}

	@Override
	protected UniqueEntityLoader buildBatchingLoader(
			OuterJoinLoadable persister,
			int batchSize,
			LockMode lockMode,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers influencers) {
		return new ReactiveDynamicBatchingEntityDelegator( persister, batchSize, lockMode, factory, influencers );
	}

	@Override
	protected UniqueEntityLoader buildBatchingLoader(
			OuterJoinLoadable persister,
			int batchSize,
			LockOptions lockOptions,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers influencers) {
		return new ReactiveDynamicBatchingEntityDelegator( persister, batchSize, lockOptions, factory, influencers );
	}

	private CompletionStage<List<Object>> performOrderedMultiLoad(
			OuterJoinLoadable persister,
			Serializable[] ids,
			SessionImplementor session,
			MultiLoadOptions loadOptions) {
		assert loadOptions.isOrderReturnEnabled();

		final List<Object> result = CollectionHelper.arrayList( ids.length );

		final LockOptions lockOptions = loadOptions.getLockOptions() == null
				? new LockOptions( LockMode.NONE )
				: loadOptions.getLockOptions();

		final int maxBatchSize;
		if ( loadOptions.getBatchSize() != null && loadOptions.getBatchSize() > 0 ) {
			maxBatchSize = loadOptions.getBatchSize();
		}
		else {
			maxBatchSize = session.getJdbcServices().getJdbcEnvironment().getDialect().getDefaultBatchLoadSizingStrategy().determineOptimalBatchLoadSize(
					persister.getIdentifierType().getColumnSpan( session.getFactory() ),
					ids.length
			);
		}

		final List<Serializable> idsInBatch = new ArrayList<>();
		final List<Integer> elementPositionsLoadedByBatch = new ArrayList<>();

		CompletionStage<?> stage = CompletionStages.voidFuture();
		//TODO: Trampoline this!
		for ( int i = 0; i < ids.length; i++ ) {
			final Serializable id = ids[i];
			final EntityKey entityKey = new EntityKey( id, persister );

			if ( loadOptions.isSessionCheckingEnabled() || loadOptions.isSecondLevelCacheCheckingEnabled() ) {
				LoadEvent loadEvent = new LoadEvent(
						id,
						persister.getMappedClass().getName(),
						lockOptions,
						(EventSource) session,
						null
				);

				Object managedEntity = null;

				if ( loadOptions.isSessionCheckingEnabled() ) {
					// look for it in the Session first
					CacheEntityLoaderHelper.PersistenceContextEntry persistenceContextEntry =
							CacheEntityLoaderHelper.INSTANCE
									.loadFromSessionCache(
											loadEvent,
											entityKey,
											LoadEventListener.GET
									);
					managedEntity = persistenceContextEntry.getEntity();

					if ( managedEntity != null
							&& !loadOptions.isReturnOfDeletedEntitiesEnabled()
							&& !persistenceContextEntry.isManaged() ) {
						// put a null in the result
						result.add( i, null );
						continue;
					}
				}

				if ( managedEntity == null && loadOptions.isSecondLevelCacheCheckingEnabled() ) {
					// look for it in the SessionFactory
					managedEntity = CacheEntityLoaderHelper.INSTANCE
							.loadFromSecondLevelCache(
									loadEvent,
									persister,
									entityKey
							);
				}

				if ( managedEntity != null ) {
					result.add( i, managedEntity );
					continue;
				}
			}

			// if we did not hit any of the continues above, then we need to batch
			// load the entity state.
			idsInBatch.add( ids[i] );

			if ( idsInBatch.size() >= maxBatchSize ) {
				CompletionStage<List<Object>> load = performOrderedBatchLoad(idsInBatch, lockOptions, persister, session);
				stage = stage.thenCompose( v -> load );
			}

			// Save the EntityKey instance for use later!
			result.add( i, entityKey );
			elementPositionsLoadedByBatch.add( i );
		}

		if ( !idsInBatch.isEmpty() ) {
			CompletionStage<List<Object>> load = performOrderedBatchLoad(idsInBatch, lockOptions, persister, session);
			stage = stage.thenCompose( v -> load );
		}

		return stage.thenApply( v -> {
			final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
			for ( Integer position : elementPositionsLoadedByBatch ) {
				// the element value at this position in the result List should be
				// the EntityKey for that entity; reuse it!
				final EntityKey entityKey = (EntityKey) result.get( position );
				Object entity = persistenceContext.getEntity( entityKey );
				if ( entity != null && !loadOptions.isReturnOfDeletedEntitiesEnabled() ) {
					// make sure it is not DELETED
					final EntityEntry entry = persistenceContext.getEntry( entity );
					if ( entry.getStatus() == Status.DELETED || entry.getStatus() == Status.GONE ) {
						// the entity is locally deleted, and the options ask that we not return such entities...
						entity = null;
					}
				}
				result.set( position, entity );
			}
			return result;
		});
	}

}
