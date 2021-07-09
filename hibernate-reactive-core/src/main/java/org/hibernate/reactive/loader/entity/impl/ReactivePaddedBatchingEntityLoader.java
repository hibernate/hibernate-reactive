/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.entity.impl;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.engine.internal.BatchFetchQueueHelper;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.util.collections.ArrayHelper;
import org.hibernate.persister.entity.OuterJoinLoadable;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;

/**
 * A batching entity loader for {@link org.hibernate.loader.BatchFetchStyle#PADDED}.
 *
 * @see org.hibernate.loader.entity.plan.PaddedBatchingEntityLoaderBuilder
 * @see org.hibernate.loader.entity.plan.PaddedBatchingEntityLoader
 *
 */
public class ReactivePaddedBatchingEntityLoader extends ReactiveBatchingEntityLoader {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final int[] batchSizes;
	private final ReactiveEntityLoader[] loaders;

	public ReactivePaddedBatchingEntityLoader(
			OuterJoinLoadable persister,
			int maxBatchSize,
			LockMode lockMode,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) {
		super( persister );
		this.batchSizes = ArrayHelper.getBatchSizes( maxBatchSize );
		this.loaders = new ReactiveEntityLoader[ batchSizes.length ];
		for ( int i = 0; i < batchSizes.length; i++ ) {
			this.loaders[i] = new ReactiveEntityLoader( persister, batchSizes[i], lockMode, factory, loadQueryInfluencers);
		}
		validate( maxBatchSize );
	}

	public ReactivePaddedBatchingEntityLoader(
			OuterJoinLoadable persister,
			int maxBatchSize,
			LockOptions lockOptions,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) {
		super( persister );
		this.batchSizes = ArrayHelper.getBatchSizes( maxBatchSize );
		this.loaders = new ReactiveEntityLoader[ batchSizes.length ];
		for ( int i = 0; i < batchSizes.length; i++ ) {
			this.loaders[i] = new ReactiveEntityLoader( persister, batchSizes[i], lockOptions, factory, loadQueryInfluencers);
		}
		validate( maxBatchSize );
	}

	@Override
	public CompletionStage<Object> load(Serializable id, Object optionalObject, SharedSessionContractImplementor session, LockOptions lockOptions) {
		return load( id, optionalObject, session, lockOptions, null );
	}

	private void validate(int max) {
		// these are more indicative of internal problems then user error...
		if ( batchSizes[0] != max ) {
			throw LOG.unexpectedBatchSizeSpread();
		}
		if ( batchSizes[batchSizes.length-1] != 1 ) {
			throw LOG.unexpectedBatchSizeSpread();
		}
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
}
