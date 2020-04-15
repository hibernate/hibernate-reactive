/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.rx.loader.entity.impl;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.engine.internal.BatchFetchQueueHelper;
import org.hibernate.engine.spi.*;
import org.hibernate.internal.util.collections.ArrayHelper;
import org.hibernate.loader.Loader;
import org.hibernate.loader.entity.BatchingEntityLoader;
import org.hibernate.loader.entity.UniqueEntityLoader;
import org.hibernate.persister.entity.OuterJoinLoadable;
import org.hibernate.pretty.MessageHelper;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.concurrent.CompletionStage;

/**
* see org.hibernate.loader.entity.PaddedBatchingEntityLoaderBuilder
*/
public class RxPaddedBatchingEntityLoaderBuilder extends RxBatchingEntityLoaderBuilder {
	public static final RxPaddedBatchingEntityLoaderBuilder INSTANCE = new RxPaddedBatchingEntityLoaderBuilder();

	@Override
	protected UniqueEntityLoader buildBatchingLoader(
			OuterJoinLoadable persister,
			int batchSize,
			LockMode lockMode,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers influencers) {
		return new RxPaddedBatchingEntityLoader( persister, batchSize, lockMode, factory, influencers );
	}

	@Override
	protected UniqueEntityLoader buildBatchingLoader(
			OuterJoinLoadable persister,
			int batchSize,
			LockOptions lockOptions,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers influencers) {
		return new RxPaddedBatchingEntityLoader( persister, batchSize, lockOptions, factory, influencers );
	}

	public static class RxPaddedBatchingEntityLoader extends BatchingEntityLoader {
		private final int[] batchSizes;
		private final RxEntityLoader[] loaders;

		public RxPaddedBatchingEntityLoader(
				OuterJoinLoadable persister,
				int maxBatchSize,
				LockMode lockMode,
				SessionFactoryImplementor factory,
				LoadQueryInfluencers loadQueryInfluencers) {
			super( persister );
			this.batchSizes = ArrayHelper.getBatchSizes( maxBatchSize );
			this.loaders = new RxEntityLoader[ batchSizes.length ];
			for ( int i = 0; i < batchSizes.length; i++ ) {
				this.loaders[i] = new RxEntityLoader( persister, batchSizes[i], lockMode, factory, loadQueryInfluencers);
			}
			validate( maxBatchSize );
		}

		public RxPaddedBatchingEntityLoader(
				OuterJoinLoadable persister,
				int maxBatchSize,
				LockOptions lockOptions,
				SessionFactoryImplementor factory,
				LoadQueryInfluencers loadQueryInfluencers) {
			super( persister );
			this.batchSizes = ArrayHelper.getBatchSizes( maxBatchSize );
			this.loaders = new RxEntityLoader[ batchSizes.length ];
			for ( int i = 0; i < batchSizes.length; i++ ) {
				this.loaders[i] = new RxEntityLoader( persister, batchSizes[i], lockOptions, factory, loadQueryInfluencers);
			}
			validate( maxBatchSize );
		}

		private void validate(int max) {
			// these are more indicative of internal problems then user error...
			if ( batchSizes[0] != max ) {
				throw new HibernateException( "Unexpected batch size spread" );
			}
			if ( batchSizes[batchSizes.length-1] != 1 ) {
				throw new HibernateException( "Unexpected batch size spread" );
			}
		}

		@Override
		public CompletionStage<Object> load(Serializable id, Object optionalObject, SharedSessionContractImplementor session, LockOptions lockOptions) {
			return load( id, optionalObject, session, lockOptions, null );
		}

		@Override
		public CompletionStage<Object> load(Serializable id, Object optionalObject, SharedSessionContractImplementor session, LockOptions lockOptions, Boolean readOnly) {
			final Serializable[] batch = session.getPersistenceContextInternal()
					.getBatchFetchQueue()
					.getEntityBatch( persister(), id, batchSizes[0], persister().getEntityMode() );

			final int numberOfIds = ArrayHelper.countNonNull( batch );
			if ( numberOfIds <= 1 ) {
				return loaders[batchSizes.length-1]
						.load( id, optionalObject, session )
						.thenApply( optional -> {
							if ( optional == null ) {
								// There was no entity with the specified ID. Make sure the EntityKey does not remain
								// in the batch to avoid including it in future batches that get executed.
								BatchFetchQueueHelper.removeBatchLoadableEntityKey( id, persister(), session );
							}
							return optional;
						});
			}

			// Uses the first batch-size bigger than the number of actual ids in the batch
			int indexToUse = batchSizes.length-1;
			for ( int i = 0; i < batchSizes.length-1; i++ ) {
				if ( batchSizes[i] >= numberOfIds ) {
					indexToUse = i;
				}
				else {
					break;
				}
			}

			final Serializable[] idsToLoad = new Serializable[ batchSizes[indexToUse] ];
			System.arraycopy( batch, 0, idsToLoad, 0, numberOfIds );
			for ( int i = numberOfIds; i < batchSizes[indexToUse]; i++ ) {
				idsToLoad[i] = id;
			}

			return doBatchLoad( id, loaders[indexToUse], session, idsToLoad, optionalObject, lockOptions, readOnly );
		}

		/**
		 * @see BatchingEntityLoader#doBatchLoad(Serializable, Loader, SharedSessionContractImplementor, Serializable[], Object, LockOptions, Boolean)
		 */
		protected CompletionStage<Object> doBatchLoad(
				Serializable id,
				RxEntityLoader loaderToUse,
				SharedSessionContractImplementor session,
				Serializable[] ids,
				Object optionalObject,
				LockOptions lockOptions,
				Boolean readOnly) {
//			if ( log.isDebugEnabled() ) {
//				log.debugf( "Batch loading entity: %s", MessageHelper.infoString( persister, ids, session.getFactory() ) );
//			}

			QueryParameters qp = buildQueryParameters(id, ids, optionalObject, lockOptions, readOnly);
			return loaderToUse.doRxQueryAndInitializeNonLazyCollections((SessionImplementor) session, qp, false)
					.handle((list, e) -> {
//						log.debug( "Done entity batch load" );
						// The EntityKey for any entity that is not found will remain in the batch.
						// Explicitly remove the EntityKeys for entities that were not found to
						// avoid including them in future batches that get executed.
						Object result = getObjectFromList(list, id, session);

						BatchFetchQueueHelper.removeNotFoundBatchLoadableEntityKeys(
								ids,
								list,
								persister(),
								session
						);
						if (e instanceof SQLException) {
							throw session.getJdbcServices().getSqlExceptionHelper().convert(
									(SQLException) e,
									"could not load an entity batch: " + MessageHelper.infoString(persister(), ids, session.getFactory()),
									loaderToUse.getSQLString()
							);
						}
						return result;
					});
		}
	}

}
