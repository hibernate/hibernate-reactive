/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.boot.spi;

import java.util.Collection;

import org.hibernate.boot.spi.AbstractDelegatingMetadata;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.query.named.NamedObjectRepository;
import org.hibernate.reactive.query.internal.ReactiveNamedObjectRepositoryImpl;

public class ReactiveMetadataImplementor extends AbstractDelegatingMetadata {

	public ReactiveMetadataImplementor(MetadataImplementor delegate) {
		super( delegate );
	}

	@Override
	public NamedObjectRepository buildNamedQueryRepository(SessionFactoryImplementor sessionFactory) {
		return new ReactiveNamedObjectRepositoryImpl( delegate().buildNamedQueryRepository( sessionFactory ) );
	}

	@Override
	public Collection<PersistentClass> getEntityBindings() {
		Collection<PersistentClass> entityBindings = super.getEntityBindings();

		return entityBindings;
	}
}
