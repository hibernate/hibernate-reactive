/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.impl;


import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.mutation.MergeCoordinator;
import org.hibernate.reactive.persister.entity.mutation.ReactiveMergeCoordinator;
import org.hibernate.reactive.persister.entity.mutation.ReactiveScopedUpdateCoordinator;
import org.hibernate.reactive.persister.entity.mutation.ReactiveUpdateCoordinator;
import org.hibernate.reactive.sql.model.ReactiveDeleteOrUpsertOperation;
import org.hibernate.reactive.sql.model.ReactiveOptionalTableUpdateOperation;
import org.hibernate.sql.model.MutationOperation;
import org.hibernate.sql.model.ValuesAnalysis;
import org.hibernate.sql.model.ast.TableMutation;
import org.hibernate.sql.model.internal.OptionalTableUpdate;
import org.hibernate.sql.model.jdbc.DeleteOrUpsertOperation;
import org.hibernate.sql.model.jdbc.OptionalTableUpdateOperation;

public class ReactiveMergeCoordinatorStandardScopeFactory extends MergeCoordinator
		implements ReactiveUpdateCoordinator {

	public ReactiveMergeCoordinatorStandardScopeFactory(
			AbstractEntityPersister entityPersister,
			SessionFactoryImplementor factory) {
		super( entityPersister, factory );
	}

	@Override
	public ReactiveScopedUpdateCoordinator makeScopedCoordinator() {
		return new ReactiveMergeCoordinator(
				entityPersister(),
				factory(),
				getStaticMutationOperationGroup(),
				getBatchKey(),
				getVersionUpdateGroup(),
				getVersionUpdateBatchkey()
		);
	}

	@Override
	protected MutationOperation createOperation(ValuesAnalysis valuesAnalysis, TableMutation<?> singleTableMutation) {
		MutationOperation operation = singleTableMutation.createMutationOperation( valuesAnalysis, factory() );
		if ( operation instanceof OptionalTableUpdateOperation ) {
			// We need to plug in our own reactive operation
			return new ReactiveOptionalTableUpdateOperation( operation.getMutationTarget(), (OptionalTableUpdate) singleTableMutation, factory() );
		}
		if ( operation instanceof DeleteOrUpsertOperation ) {
			return new ReactiveDeleteOrUpsertOperation( (DeleteOrUpsertOperation) operation );
		}
		return operation;
	}
}
