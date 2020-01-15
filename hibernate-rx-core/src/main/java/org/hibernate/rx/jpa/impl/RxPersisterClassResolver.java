package org.hibernate.rx.jpa.impl;

import org.hibernate.mapping.Collection;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.internal.StandardPersisterClassResolver;
import org.hibernate.persister.spi.PersisterClassResolver;
import org.hibernate.rx.persister.impl.RxJoinedSubclassEntityPersister;
import org.hibernate.rx.persister.impl.RxSingleTableEntityPersister;
import org.hibernate.rx.persister.impl.RxUnionSubclassEntityPersister;

public class RxPersisterClassResolver extends StandardPersisterClassResolver implements PersisterClassResolver {

	@Override
	public Class<? extends EntityPersister> singleTableEntityPersister() {
		return RxSingleTableEntityPersister.class;
	}

	@Override
	public Class<? extends EntityPersister> joinedSubclassEntityPersister() {
		return RxJoinedSubclassEntityPersister.class;
	}

	@Override
	public Class<? extends EntityPersister> unionSubclassEntityPersister() {
		return RxUnionSubclassEntityPersister.class;
	}

	@Override
	public Class<? extends CollectionPersister> getCollectionPersisterClass(Collection metadata) {
		throw new UnsupportedOperationException( "Collections not supported" );
	}
}