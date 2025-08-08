/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.cte;

import org.hibernate.internal.util.MutableObject;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.mutation.internal.InsertHandler;
import org.hibernate.query.sqm.mutation.internal.cte.CteInsertStrategy;
import org.hibernate.query.sqm.mutation.spi.MultiTableHandlerBuildResult;
import org.hibernate.query.sqm.tree.insert.SqmInsertStatement;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveSqmMultiTableInsertStrategy;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

public class ReactiveCteInsertStrategy extends CteInsertStrategy implements ReactiveSqmMultiTableInsertStrategy {

	public ReactiveCteInsertStrategy(
			EntityMappingType rootEntityType,
			RuntimeModelCreationContext runtimeModelCreationContext) {
		super( rootEntityType, runtimeModelCreationContext );
	}

	public ReactiveCteInsertStrategy(
			EntityPersister rootDescriptor,
			RuntimeModelCreationContext runtimeModelCreationContext) {
		super( rootDescriptor, runtimeModelCreationContext );
	}

	@Override
	public MultiTableHandlerBuildResult buildHandler(SqmInsertStatement<?> sqmInsertStatement, DomainParameterXref domainParameterXref, DomainQueryExecutionContext context) {
		final MutableObject<JdbcParameterBindings> firstJdbcParameterBindings = new MutableObject<>();
		final InsertHandler multiTableHandler = new ReactiveCteInsertHandler(
				getEntityCteTable(),
				sqmInsertStatement,
				domainParameterXref,
				context,
				firstJdbcParameterBindings
		);
		return new MultiTableHandlerBuildResult( multiTableHandler, firstJdbcParameterBindings.get() );
	}

}
