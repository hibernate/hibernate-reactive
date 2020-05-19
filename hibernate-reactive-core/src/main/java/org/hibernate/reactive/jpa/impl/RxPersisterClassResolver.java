package org.hibernate.reactive.jpa.impl;

import org.hibernate.mapping.Collection;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.internal.StandardPersisterClassResolver;
import org.hibernate.persister.spi.PersisterClassResolver;
import org.hibernate.reactive.persister.entity.impl.RxJoinedSubclassEntityPersister;
import org.hibernate.reactive.persister.entity.impl.RxSingleTableEntityPersister;
import org.hibernate.reactive.persister.entity.impl.RxUnionSubclassEntityPersister;
import org.hibernate.reactive.persister.persister.collection.RxOneToManyPersister;

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
		return RxOneToManyPersister.class;
	}
}