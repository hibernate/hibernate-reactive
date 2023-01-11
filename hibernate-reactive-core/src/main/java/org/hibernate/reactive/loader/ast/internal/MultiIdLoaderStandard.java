/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.ast.spi.MultiIdLoadOptions;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.loader.ast.spi.ReactiveMultiIdEntityLoader;

import org.jboss.logging.Logger;

public class MultiIdLoaderStandard<T> implements ReactiveMultiIdEntityLoader<T> {
	private static final Logger log = Logger.getLogger( MultiIdLoaderStandard.class );

	private final EntityPersister entityDescriptor;
	private final SessionFactoryImplementor sessionFactory;

	private final int idJdbcTypeCount;

	public MultiIdLoaderStandard(
			EntityPersister entityDescriptor,
			PersistentClass bootDescriptor,
			SessionFactoryImplementor sessionFactory) {
		this.entityDescriptor = entityDescriptor;
		this.idJdbcTypeCount = bootDescriptor.getIdentifier().getColumnSpan();
		this.sessionFactory = sessionFactory;
		assert idJdbcTypeCount > 0;
	}

	@Override
	public EntityMappingType getLoadable() {
		return entityDescriptor;
	}

	@Override
	public CompletionStage<List<T>> load(Object[] ids, MultiIdLoadOptions loadOptions, SharedSessionContractImplementor session) {
		assert ids != null;

		if ( loadOptions.isOrderReturnEnabled() ) {
			return performOrderedMultiLoad( ids, session, loadOptions );
		}
		else {
			return performUnorderedMultiLoad( ids, session, loadOptions );
		}
	}

	private CompletionStage<List<T>> performOrderedMultiLoad(
			Object[] ids,
			SharedSessionContractImplementor session,
			MultiIdLoadOptions loadOptions) {
		throw new UnsupportedOperationException();
	}

	private CompletionStage<List<T>> performUnorderedMultiLoad(
			Object[] ids,
			SharedSessionContractImplementor session,
			MultiIdLoadOptions loadOptions) {
		throw new UnsupportedOperationException();
	}
}
