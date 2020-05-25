/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
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
 * A {@link ReactiveBatchingEntityLoaderBuilder} that is enabled when
 * {@link org.hibernate.loader.BatchFetchStyle#PADDED} is selected.
 *
 * A factory for {@link ReactivePaddedBatchingEntityLoader}s.
 *
 * @see org.hibernate.loader.entity.PaddedBatchingEntityLoaderBuilder
*/
public class ReactivePaddedBatchingEntityLoaderBuilder extends ReactiveBatchingEntityLoaderBuilder {

	public static final ReactivePaddedBatchingEntityLoaderBuilder INSTANCE = new ReactivePaddedBatchingEntityLoaderBuilder();

	@Override
	protected UniqueEntityLoader buildBatchingLoader(
			OuterJoinLoadable persister,
			int batchSize,
			LockMode lockMode,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers influencers) {
		return new ReactivePaddedBatchingEntityLoader( persister, batchSize, lockMode, factory, influencers );
	}

	@Override
	protected UniqueEntityLoader buildBatchingLoader(
			OuterJoinLoadable persister,
			int batchSize,
			LockOptions lockOptions,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers influencers) {
		return new ReactivePaddedBatchingEntityLoader( persister, batchSize, lockOptions, factory, influencers );
	}

}
