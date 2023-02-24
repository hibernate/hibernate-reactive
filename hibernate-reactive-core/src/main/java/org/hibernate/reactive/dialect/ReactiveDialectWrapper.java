/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.dialect;

import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.DialectDelegateWrapper;
import org.hibernate.dialect.identity.IdentityColumnSupport;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableInsertStrategy;
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableMutationStrategy;
import org.hibernate.reactive.dialect.identity.ReactiveIdentityColumnSupportAdapter;

/**
 * Wraps the given dialect to make some internal components reactive;
 * also, potentially applies a SQL syntax workaround if the wrapped Dialect
 * is extending PostgreSQLDialect.
 */
public final class ReactiveDialectWrapper extends DialectDelegateWrapper {

	public ReactiveDialectWrapper(Dialect wrapped) {
		super( wrapped );
	}

	@Override
	public IdentityColumnSupport getIdentityColumnSupport() {
		return new ReactiveIdentityColumnSupportAdapter( super.getIdentityColumnSupport() );
	}

	@Override
	public SqmMultiTableMutationStrategy getFallbackSqmMutationStrategy(
			EntityMappingType rootEntityDescriptor,
			RuntimeModelCreationContext runtimeModelCreationContext) {
		throw new IllegalStateException(
				"Should never be called - see ReactiveSqmMultiTableMutationStrategyProvider" );
	}

	@Override
	public SqmMultiTableInsertStrategy getFallbackSqmInsertStrategy(
			EntityMappingType rootEntityDescriptor,
			RuntimeModelCreationContext runtimeModelCreationContext) {
		throw new IllegalStateException(
				"Should never be called - see ReactiveSqmMultiTableMutationStrategyProvider" );

	}

}
