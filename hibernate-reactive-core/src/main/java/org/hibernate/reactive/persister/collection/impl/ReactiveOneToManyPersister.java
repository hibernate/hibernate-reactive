package org.hibernate.reactive.persister.collection.impl;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.MappingException;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.CollectionDataAccess;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.SubselectFetch;
import org.hibernate.loader.collection.CollectionInitializer;
import org.hibernate.mapping.Collection;
import org.hibernate.persister.collection.OneToManyPersister;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.hibernate.reactive.loader.collection.impl.ReactiveBatchingCollectionInitializerBuilder;
import org.hibernate.reactive.loader.collection.impl.ReactiveCollectionInitializer;
import org.hibernate.reactive.loader.collection.impl.ReactiveCollectionLoader;
import org.hibernate.reactive.loader.collection.impl.ReactiveSubselectOneToManyLoader;

public class ReactiveOneToManyPersister extends OneToManyPersister {
	public ReactiveOneToManyPersister(Collection collectionBinding, CollectionDataAccess cacheAccessStrategy, PersisterCreationContext creationContext) throws MappingException, CacheException {
		super( collectionBinding, cacheAccessStrategy, creationContext );
	}

	public CompletionStage<Void> reactiveInitialize(Serializable key, SharedSessionContractImplementor session)
			throws HibernateException {
		return getAppropriateInitializer( key, session ).reactiveInitialize( key, session );
	}

	@Override
	protected ReactiveCollectionInitializer createCollectionInitializer(LoadQueryInfluencers loadQueryInfluencers)
			throws MappingException {
		return ReactiveBatchingCollectionInitializerBuilder.getBuilder( getFactory() )
				.createBatchingOneToManyInitializer( this, batchSize, getFactory(), loadQueryInfluencers );
	}

	@Override
	protected ReactiveCollectionInitializer createSubselectInitializer(SubselectFetch subselect, SharedSessionContractImplementor session) {
		return new ReactiveSubselectOneToManyLoader(
				this,
				subselect.toSubselectString( getCollectionType().getLHSPropertyName() ),
				subselect.getResult(),
				subselect.getQueryParameters(),
				subselect.getNamedParameterLocMap(),
				session.getFactory(),
				session.getLoadQueryInfluencers()
		);
	}

	protected ReactiveCollectionInitializer getAppropriateInitializer(Serializable key, SharedSessionContractImplementor session) {
		return (ReactiveCollectionInitializer) super.getAppropriateInitializer(key, session);
	}
}
