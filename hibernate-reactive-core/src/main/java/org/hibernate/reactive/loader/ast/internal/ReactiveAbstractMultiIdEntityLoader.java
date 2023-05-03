/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.loader.ast.internal.Preparable;
import org.hibernate.loader.ast.spi.MultiIdLoadOptions;
import org.hibernate.metamodel.mapping.EntityIdentifierMapping;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.reactive.loader.ast.spi.ReactiveMultiIdEntityLoader;

/**
 * @see org.hibernate.loader.ast.internal.AbstractMultiIdEntityLoader
 */
public abstract class ReactiveAbstractMultiIdEntityLoader<T> implements ReactiveMultiIdEntityLoader<T>, Preparable {

	private final EntityMappingType entityDescriptor;
	private final SessionFactoryImplementor sessionFactory;

	private EntityIdentifierMapping identifierMapping;

	public ReactiveAbstractMultiIdEntityLoader(EntityMappingType entityDescriptor, SessionFactoryImplementor sessionFactory) {
		this.entityDescriptor = entityDescriptor;
		this.sessionFactory = sessionFactory;
	}

	@Override
	public void prepare() {
		identifierMapping = getLoadable().getIdentifierMapping();
	}

	protected EntityMappingType getEntityDescriptor() {
		return entityDescriptor;
	}

	protected SessionFactoryImplementor getSessionFactory() {
		return sessionFactory;
	}

	public EntityIdentifierMapping getIdentifierMapping() {
		return identifierMapping;
	}

	@Override
	public EntityMappingType getLoadable() {
		return getEntityDescriptor();
	}

	@Override
	public final <K> CompletionStage<List<T>> load(K[] ids, MultiIdLoadOptions loadOptions, EventSource session) {
		Objects.requireNonNull( ids );

		return loadOptions.isOrderReturnEnabled()
				? performOrderedMultiLoad( ids, loadOptions, session )
				: performUnorderedMultiLoad( ids, loadOptions, session );
	}

	protected abstract <K> CompletionStage<List<T>> performOrderedMultiLoad(K[] ids, MultiIdLoadOptions loadOptions, EventSource session);

	protected abstract <K> CompletionStage<List<T>> performUnorderedMultiLoad(K[] ids, MultiIdLoadOptions loadOptions, EventSource session);

}
