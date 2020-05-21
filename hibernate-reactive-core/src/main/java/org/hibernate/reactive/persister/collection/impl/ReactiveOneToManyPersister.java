package org.hibernate.reactive.persister.collection.impl;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.MappingException;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.CollectionDataAccess;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.collection.BatchingCollectionInitializerBuilder;
import org.hibernate.loader.collection.CollectionInitializer;
import org.hibernate.mapping.Collection;
import org.hibernate.persister.collection.OneToManyPersister;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.hibernate.reactive.loader.collection.impl.ReactiveCollectionLoader;
import org.hibernate.reactive.loader.collection.impl.ReactiveOneToManyLoader;

public class ReactiveOneToManyPersister extends OneToManyPersister {
	public ReactiveOneToManyPersister(Collection collectionBinding, CollectionDataAccess cacheAccessStrategy, PersisterCreationContext creationContext) throws MappingException, CacheException {
		super( collectionBinding, cacheAccessStrategy, creationContext );
	}

	public CompletionStage<Void> reactiveInitialize(Serializable key, SharedSessionContractImplementor session)
			throws HibernateException {
		return ( (ReactiveCollectionLoader) getAppropriateInitializer( key, session ) ).reactiveInitialize( key, session );
	}

	@Override
	protected CollectionInitializer createCollectionInitializer(LoadQueryInfluencers loadQueryInfluencers)
			throws MappingException {
		CollectionInitializer initializer = BatchingCollectionInitializerBuilder.getBuilder( getFactory() )
				.createBatchingOneToManyInitializer( this, batchSize, getFactory(), loadQueryInfluencers );
		return new ReactiveOneToManyLoader( this, getFactory(), loadQueryInfluencers );
	}
}
