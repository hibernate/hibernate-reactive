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
import org.hibernate.reactive.sql.model.ReactiveOptionalTableUpdateOperation;
import org.hibernate.sql.model.ModelMutationLogging;
import org.hibernate.sql.model.MutationOperation;
import org.hibernate.sql.model.MutationOperationGroup;
import org.hibernate.sql.model.ValuesAnalysis;
import org.hibernate.sql.model.ast.MutationGroup;
import org.hibernate.sql.model.ast.TableMutation;
import org.hibernate.sql.model.internal.MutationOperationGroupFactory;
import org.hibernate.sql.model.internal.OptionalTableUpdate;
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

	// We override the whole method but we just need to plug in our custom createOperation(...) method
	@Override
	protected MutationOperationGroup createOperationGroup(ValuesAnalysis valuesAnalysis, MutationGroup mutationGroup) {
		final int numberOfTableMutations = mutationGroup.getNumberOfTableMutations();
		switch ( numberOfTableMutations ) {
			case 0:
				return MutationOperationGroupFactory.noOperations( mutationGroup );
			case 1: {
				MutationOperation operation = createOperation( valuesAnalysis, mutationGroup.getSingleTableMutation() );
				return operation == null
						? MutationOperationGroupFactory.noOperations( mutationGroup )
						: MutationOperationGroupFactory.singleOperation( mutationGroup, operation );
			}
			default: {
				MutationOperation[] operations = new MutationOperation[numberOfTableMutations];
				int outputIndex = 0;
				int skipped = 0;
				for ( int i = 0; i < mutationGroup.getNumberOfTableMutations(); i++ ) {
					final TableMutation<?> tableMutation = mutationGroup.getTableMutation( i );
					MutationOperation operation = createOperation( valuesAnalysis, tableMutation );
					if ( operation != null ) {
						operations[outputIndex++] = operation;
					}
					else {
						skipped++;
						ModelMutationLogging.MODEL_MUTATION_LOGGER.debugf(
								"Skipping table update - %s",
								tableMutation.getTableName()
						);
					}
				}
				if ( skipped != 0 ) {
					final MutationOperation[] trimmed = new MutationOperation[outputIndex];
					System.arraycopy( operations, 0, trimmed, 0, outputIndex );
					operations = trimmed;
				}
				return MutationOperationGroupFactory.manyOperations(
						mutationGroup.getMutationType(),
						entityPersister,
						operations
				);
			}
		}
	}

	// FIXME: We could add this method in ORM and override only this code
	protected MutationOperation createOperation(ValuesAnalysis valuesAnalysis, TableMutation<?> singleTableMutation) {
		MutationOperation operation = singleTableMutation.createMutationOperation( valuesAnalysis, factory() );
		if ( operation instanceof OptionalTableUpdateOperation ) {
			// We need to plug in our own reactive operation
			return new ReactiveOptionalTableUpdateOperation(
					operation.getMutationTarget(),
					(OptionalTableUpdate) singleTableMutation,
					factory()
			);
		}
		return operation;
	}
}
