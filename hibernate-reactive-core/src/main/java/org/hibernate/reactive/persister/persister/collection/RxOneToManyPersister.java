package org.hibernate.reactive.persister.persister.collection;

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
import org.hibernate.reactive.loader.collection.RxCollectionLoader;
import org.hibernate.reactive.loader.collection.RxOneToManyLoader;

public class RxOneToManyPersister extends OneToManyPersister {
	public RxOneToManyPersister(Collection collectionBinding, CollectionDataAccess cacheAccessStrategy, PersisterCreationContext creationContext) throws MappingException, CacheException {
		super( collectionBinding, cacheAccessStrategy, creationContext );
	}

	public CompletionStage<Void> rxInitialize(Serializable key, SharedSessionContractImplementor session)
			throws HibernateException {
		return ( (RxCollectionLoader) getAppropriateInitializer( key, session ) ).rxInitialize( key, session );
	}

	@Override
	protected CollectionInitializer createCollectionInitializer(LoadQueryInfluencers loadQueryInfluencers)
			throws MappingException {
		CollectionInitializer initializer = BatchingCollectionInitializerBuilder.getBuilder( getFactory() )
				.createBatchingOneToManyInitializer( this, batchSize, getFactory(), loadQueryInfluencers );
		return new RxOneToManyLoader( this, getFactory(), loadQueryInfluencers );
	}
}
