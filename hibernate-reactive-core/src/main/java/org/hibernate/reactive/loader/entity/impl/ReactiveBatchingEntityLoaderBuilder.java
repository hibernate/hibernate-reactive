/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.reactive.loader.entity.impl;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.loader.entity.UniqueEntityLoader;
import org.hibernate.persister.entity.OuterJoinLoadable;

/**
 * Superclass of builders for batching entity loaders.
 *
 * The {@link #getBuilder(SessionFactoryImplementor)} method selects
 * between {@link ReactivePaddedBatchingEntityLoaderBuilder} and
 * {@link ReactiveDynamicBatchingEntityLoaderBuilder} depending upon
 * the {@link org.hibernate.loader.BatchFetchStyle} selected.
 *
 * @see org.hibernate.loader.entity.BatchingEntityLoaderBuilder
 */
public class ReactiveBatchingEntityLoaderBuilder {

	public static ReactiveBatchingEntityLoaderBuilder getBuilder(SessionFactoryImplementor factory) {
		switch ( factory.getSessionFactoryOptions().getBatchFetchStyle() ) {
			case PADDED:
				return ReactivePaddedBatchingEntityLoaderBuilder.INSTANCE;
			case DYNAMIC:
				return ReactiveDynamicBatchingEntityLoaderBuilder.INSTANCE;
			default:
				//we don't have support for the "legacy" (default) style yet
				//return LegacyBatchingEntityLoaderBuilder.INSTANCE;
				return ReactiveDynamicBatchingEntityLoaderBuilder.INSTANCE;
		}
	}

	/**
	 * Builds a batch-fetch capable loader based on the given persister, lock-mode, etc.
	 *
	 * @param persister The entity persister
	 * @param batchSize The maximum number of ids to batch-fetch at once
	 * @param lockMode The lock mode
	 * @param factory The SessionFactory
	 * @param influencers Any influencers that should affect the built query
	 *
	 * @return The loader.
	 */
	public UniqueEntityLoader buildLoader(
			OuterJoinLoadable persister,
			int batchSize,
			LockMode lockMode,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers influencers) {
		if ( batchSize <= 1 ) {
			// no batching
			return buildNonBatchingLoader( persister, lockMode, factory, influencers );
		}
		return buildBatchingLoader( persister, batchSize, lockMode, factory, influencers );
	}

	protected UniqueEntityLoader buildNonBatchingLoader(
			OuterJoinLoadable persister,
			LockMode lockMode,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers influencers) {
		return new ReactivePlanEntityLoader.Builder( persister )
				.withLockMode( lockMode )
				.withInfluencers( influencers )
				.byPrimaryKey();
//		return new ReactiveEntityLoader( persister, factory, lockMode, influencers);
	}

	/**
	 * Builds a batch-fetch capable loader based on the given persister, lock-options, etc.
	 *
	 * @param persister The entity persister
	 * @param batchSize The maximum number of ids to batch-fetch at once
	 * @param lockOptions The lock options
	 * @param factory The SessionFactory
	 * @param influencers Any influencers that should affect the built query
	 *
	 * @return The loader.
	 */
	public UniqueEntityLoader buildLoader(
			OuterJoinLoadable persister,
			int batchSize,
			LockOptions lockOptions,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers influencers) {
		if ( batchSize <= 1 ) {
			// no batching
			return buildNonBatchingLoader( persister, lockOptions, factory, influencers );
		}
		return buildBatchingLoader( persister, batchSize, lockOptions, factory, influencers );
	}

	protected UniqueEntityLoader buildNonBatchingLoader(
			OuterJoinLoadable persister,
			LockOptions lockOptions,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers influencers) {
		return new ReactivePlanEntityLoader.Builder( persister )
				.withLockMode( lockOptions.getLockMode() )
				.withInfluencers( influencers )
				.byPrimaryKey();
//		return new ReactiveEntityLoader( persister, factory, lockOptions.getLockMode(), influencers);
	}

	protected UniqueEntityLoader buildBatchingLoader(
			OuterJoinLoadable persister,
			int batchSize,
			LockMode lockMode,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers influencers) {
		return new ReactiveEntityLoader( persister, factory, lockMode, influencers);
	}

	protected UniqueEntityLoader buildBatchingLoader(
			OuterJoinLoadable persister,
			int batchSize,
			LockOptions lockOptions,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers influencers) {
		return new ReactiveEntityLoader( persister, factory, lockOptions.getLockMode(), influencers);
	}
}
