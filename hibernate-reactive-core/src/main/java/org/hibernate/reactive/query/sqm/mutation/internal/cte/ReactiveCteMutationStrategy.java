/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal.cte;

import java.util.Locale;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.mutation.internal.cte.CteMutationStrategy;
import org.hibernate.query.sqm.tree.SqmDeleteOrUpdateStatement;
import org.hibernate.query.sqm.tree.delete.SqmDeleteStatement;
import org.hibernate.query.sqm.tree.update.SqmUpdateStatement;
import org.hibernate.reactive.query.sqm.mutation.spi.ReactiveSqmMultiTableMutationStrategy;
import org.hibernate.sql.ast.tree.cte.CteTable;

public class ReactiveCteMutationStrategy extends CteMutationStrategy implements ReactiveSqmMultiTableMutationStrategy {

	// These should be getters in the super class
	private final CteTable idCteTable;
	private final EntityPersister rootDescriptor;
	private final SessionFactoryImplementor sessionFactory;

	public ReactiveCteMutationStrategy(EntityMappingType rootEntityType, RuntimeModelCreationContext runtimeModelCreationContext) {
		this( rootEntityType.getEntityPersister(), runtimeModelCreationContext );
	}

	public ReactiveCteMutationStrategy(EntityPersister rootDescriptor, RuntimeModelCreationContext runtimeModelCreationContext) {
		super( rootDescriptor, runtimeModelCreationContext );
		this.idCteTable = CteTable.createIdTable( ID_TABLE_NAME, rootDescriptor );
		this.rootDescriptor = rootDescriptor;
		this.sessionFactory = runtimeModelCreationContext.getSessionFactory();
	}

	@Override
	public CompletionStage<Integer> reactiveExecuteUpdate(
			SqmUpdateStatement<?> sqmUpdateStatement,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext context) {
		checkMatch( sqmUpdateStatement );
		return new ReactiveCteUpdateHandler( idCteTable, sqmUpdateStatement, domainParameterXref, this, sessionFactory )
				.reactiveExecute( context );
	}

	@Override
	public CompletionStage<Integer> reactiveExecuteDelete(
			SqmDeleteStatement<?> sqmDeleteStatement,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext context) {
		checkMatch( sqmDeleteStatement );
		return new ReactiveCteDeleteHandler( idCteTable, sqmDeleteStatement, domainParameterXref, this, sessionFactory )
				.reactiveExecute( context );
	}

	private void checkMatch(SqmDeleteOrUpdateStatement<?> sqmStatement) {
		final String targetEntityName = sqmStatement.getTarget().getEntityName();
		final EntityPersister targetEntityDescriptor = sessionFactory
				.getRuntimeMetamodels()
				.getMappingMetamodel()
				.getEntityDescriptor( targetEntityName );

		if ( targetEntityDescriptor != rootDescriptor && ! rootDescriptor.isSubclassEntityName( targetEntityDescriptor.getEntityName() ) ) {
			throw new IllegalArgumentException(
					String.format(
							Locale.ROOT,
							"Target of query [%s] did not match configured entity [%s]",
							targetEntityName,
							rootDescriptor.getEntityName()
					)
			);
		}

	}
}
