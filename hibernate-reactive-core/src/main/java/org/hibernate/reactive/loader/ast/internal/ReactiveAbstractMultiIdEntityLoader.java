/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.ast.spi.MultiIdLoadOptions;
import org.hibernate.metamodel.mapping.EntityIdentifierMapping;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.reactive.loader.ast.spi.ReactiveMultiIdEntityLoader;

/**
 * @see org.hibernate.loader.ast.internal.AbstractMultiIdEntityLoader
 */
public abstract class ReactiveAbstractMultiIdEntityLoader<T> implements ReactiveMultiIdEntityLoader<T> {

	private final EntityMappingType entityDescriptor;
	private final SessionFactoryImplementor sessionFactory;

	private EntityIdentifierMapping identifierMapping;

	public ReactiveAbstractMultiIdEntityLoader(EntityMappingType entityDescriptor, SessionFactoryImplementor sessionFactory) {
		this.entityDescriptor = entityDescriptor;
		this.sessionFactory = sessionFactory;
		this.identifierMapping = getLoadable().getIdentifierMapping();
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
	public final <K> CompletionStage<List<T>> reactiveLoad(K[] ids, MultiIdLoadOptions loadOptions, SharedSessionContractImplementor session) {
		Objects.requireNonNull( ids );

		return loadOptions.isOrderReturnEnabled()
				? performOrderedMultiLoad( ids, loadOptions, session )
				: performUnorderedMultiLoad( ids, loadOptions, session );
	}

	protected abstract <K> CompletionStage<List<T>> performOrderedMultiLoad(K[] ids, MultiIdLoadOptions loadOptions, SharedSessionContractImplementor session);

	protected abstract <K> CompletionStage<List<T>> performUnorderedMultiLoad(K[] ids, MultiIdLoadOptions loadOptions, SharedSessionContractImplementor session);

	protected boolean isIdCoercionEnabled() {
		return !getSessionFactory().getSessionFactoryOptions().getJpaCompliance().isLoadByIdComplianceEnabled();
	}
}
