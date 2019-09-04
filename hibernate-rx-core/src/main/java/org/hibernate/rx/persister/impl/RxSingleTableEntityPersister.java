package org.hibernate.rx.persister.impl;

import org.hibernate.HibernateException;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.NaturalIdDataAccess;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.SingleTableEntityPersister;
import org.hibernate.persister.spi.PersisterCreationContext;

public class RxSingleTableEntityPersister extends SingleTableEntityPersister implements EntityPersister {

	public RxSingleTableEntityPersister(
			PersistentClass persistentClass,
			EntityDataAccess cacheAccessStrategy,
			NaturalIdDataAccess naturalIdRegionAccessStrategy,
			PersisterCreationContext creationContext) throws HibernateException {
		super( persistentClass, cacheAccessStrategy, naturalIdRegionAccessStrategy, creationContext );
	}
}

