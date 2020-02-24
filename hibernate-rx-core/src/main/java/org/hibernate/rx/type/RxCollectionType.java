package org.hibernate.rx.type;

import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.rx.collection.RxPersistentList;
import org.hibernate.type.CollectionType;

import java.io.Serializable;
import java.util.List;

public class RxCollectionType extends CollectionType {
	private final CollectionType delegate;

	public RxCollectionType(CollectionType delegate) {
		super(delegate.getRole(), delegate.getLHSPropertyName());
		this.delegate = delegate;
	}

	@Override
	public PersistentCollection instantiate(SharedSessionContractImplementor session, CollectionPersister persister, Serializable key) {
		PersistentCollection instantiate = delegate.instantiate( session, persister, key );
		if ( instantiate instanceof List ) {
			return new RxPersistentList( session, (List) instantiate );
		}
		return instantiate;
	}

	//////////////////
	// DELEGATES
	//////////////////

	@Override
	public PersistentCollection wrap(SharedSessionContractImplementor session, Object collection) {
		return delegate.wrap( session, collection );
	}

	@Override
	public Object instantiate(int anticipatedSize) {
		return delegate.instantiate( anticipatedSize );
	}

	@Override
	public Class getReturnedClass() {
		return delegate.getReturnedClass();
	}
}
