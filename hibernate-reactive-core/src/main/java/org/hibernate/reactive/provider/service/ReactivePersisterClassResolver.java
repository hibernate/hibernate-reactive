/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import org.hibernate.mapping.Collection;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.internal.StandardPersisterClassResolver;
import org.hibernate.persister.spi.PersisterClassResolver;
import org.hibernate.reactive.persister.collection.impl.ReactiveBasicCollectionPersister;
import org.hibernate.reactive.persister.collection.impl.ReactiveOneToManyPersister;
import org.hibernate.reactive.persister.entity.impl.ReactiveJoinedSubclassEntityPersister;
import org.hibernate.reactive.persister.entity.impl.ReactiveSingleTableEntityPersister;
import org.hibernate.reactive.persister.entity.impl.ReactiveUnionSubclassEntityPersister;

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
