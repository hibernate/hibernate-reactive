/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.loader.ast.internal.MultiKeyLoadHelper;
import org.hibernate.loader.ast.spi.BatchLoaderFactory;
import org.hibernate.loader.ast.spi.CollectionBatchLoader;
import org.hibernate.loader.ast.spi.EntityBatchLoader;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.type.BasicType;
import org.hibernate.type.SqlTypes;
import org.hibernate.type.Type;

/**
 * @see org.hibernate.loader.ast.internal.StandardBatchLoaderFactory
 */
public class ReactiveStandardBatchLoaderFactory implements BatchLoaderFactory {

	@Override
	public <T> EntityBatchLoader<T> createEntityBatchLoader(
			int domainBatchSize, EntityMappingType entityDescriptor,
			SessionFactoryImplementor factory) {
		final Dialect dialect = factory.getJdbcServices().getDialect();

		// NOTE : don't use the EntityIdentifierMapping here because it will not be known until later
		final Type identifierType = entityDescriptor.getEntityPersister().getIdentifierType();
		final int idColumnCount = identifierType.getColumnSpan( factory );

		if ( idColumnCount == 1
				&& MultiKeyLoadHelper.supportsSqlArrayType( dialect )
				&& identifierType instanceof BasicType ) {
			// we can use a single ARRAY parameter to send all the ids
			return (EntityBatchLoader<T>) new ReactiveEntityBatchLoaderArrayParam<>( domainBatchSize, entityDescriptor, factory );
		}

		final int optimalBatchSize = dialect
				.getBatchLoadSizingStrategy()
				.determineOptimalBatchLoadSize( idColumnCount, domainBatchSize, false );
		return (EntityBatchLoader<T>) new ReactiveEntityBatchLoaderInPredicate<>( domainBatchSize, optimalBatchSize, entityDescriptor, factory );
	}

	@Override
	public CollectionBatchLoader createCollectionBatchLoader(
			int domainBatchSize,
			LoadQueryInfluencers influencers,
			PluralAttributeMapping attributeMapping,
			SessionFactoryImplementor factory) {
		final Dialect dialect = factory.getJdbcServices().getDialect();
		final int columnCount = attributeMapping.getKeyDescriptor().getJdbcTypeCount();
		if ( columnCount == 1
				&& dialect.supportsStandardArrays()
				&& dialect.getPreferredSqlTypeCodeForArray() == SqlTypes.ARRAY ) {
			// we can use a single ARRAY parameter to send all the ids
			return new ReactiveCollectionBatchLoaderArrayParam( domainBatchSize, influencers, attributeMapping, factory );
		}

		return new ReactiveCollectionBatchLoaderInPredicate( domainBatchSize, influencers, attributeMapping, factory );
	}
}
