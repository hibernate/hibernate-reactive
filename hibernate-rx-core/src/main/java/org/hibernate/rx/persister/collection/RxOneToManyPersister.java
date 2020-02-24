package org.hibernate.rx.persister.collection;

import org.hibernate.HibernateException;
import org.hibernate.MappingException;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.CollectionDataAccess;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.mapping.Collection;
import org.hibernate.persister.collection.OneToManyPersister;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.hibernate.rx.type.RxCollectionType;
import org.hibernate.type.CollectionType;

import java.sql.ResultSet;
import java.sql.SQLException;

public class RxOneToManyPersister extends OneToManyPersister {
	private final RxCollectionType collectionType;

	public RxOneToManyPersister(Collection collectionBinding, CollectionDataAccess cacheAccessStrategy, PersisterCreationContext creationContext) throws MappingException, CacheException {
		super(collectionBinding, cacheAccessStrategy, creationContext);
		this.collectionType = new RxCollectionType( collectionBinding.getCollectionType() );
	}

	@Override
	public CollectionType getCollectionType() {
		return collectionType;
	}

	@Override
	public Object readElement(ResultSet rs, Object owner, String[] aliases, SharedSessionContractImplementor session) throws HibernateException, SQLException {
		Object element =  super.readElement( rs, owner, aliases, session );
		return element;
	}
}
