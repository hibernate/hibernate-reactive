/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import org.hibernate.mapping.Collection;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.internal.StandardPersisterClassResolver;
import org.hibernate.persister.spi.PersisterClassResolver;
import org.hibernate.reactive.persister.collection.internal.ReactiveBasicCollectionPersister;
import org.hibernate.reactive.persister.collection.internal.ReactiveOneToManyPersister;
import org.hibernate.reactive.persister.entity.internal.ReactiveJoinedSubclassEntityPersister;
import org.hibernate.reactive.persister.entity.internal.ReactiveSingleTableEntityPersister;
import org.hibernate.reactive.persister.entity.internal.ReactiveUnionSubclassEntityPersister;

/**
 * Define the persisters required by Hibernate Reactive, usually a reactive version of the ones
 * in Hibernate ORM.
 */
public class ReactivePersisterClassResolver extends StandardPersisterClassResolver implements PersisterClassResolver {

	@Override
	public Class<? extends EntityPersister> singleTableEntityPersister() {
		return ReactiveSingleTableEntityPersister.class;
	}

	@Override
	public Class<? extends EntityPersister> joinedSubclassEntityPersister() {
		return ReactiveJoinedSubclassEntityPersister.class;
	}

	@Override
	public Class<? extends EntityPersister> unionSubclassEntityPersister() {
		return ReactiveUnionSubclassEntityPersister.class;
	}

	@Override
	public Class<? extends CollectionPersister> getCollectionPersisterClass(Collection metadata) {
		return metadata.isOneToMany() ? oneToManyPersister() : elementCollectionPersister();
	}

	private Class<? extends CollectionPersister> oneToManyPersister() {
		return ReactiveOneToManyPersister.class;
	}

	private Class<? extends CollectionPersister> elementCollectionPersister() {
		return ReactiveBasicCollectionPersister.class;
	}
}
