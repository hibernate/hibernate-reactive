/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.mutation;

import org.hibernate.engine.jdbc.batch.spi.BatchKey;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.mutation.EntityTableMapping;
import org.hibernate.sql.model.MutationOperation;
import org.hibernate.sql.model.MutationOperationGroup;
import org.hibernate.sql.model.ast.builder.AbstractTableUpdateBuilder;
import org.hibernate.sql.model.ast.builder.TableMergeBuilder;

/**
 * @see org.hibernate.persister.entity.mutation.MergeCoordinator
 * @see org.hibernate.reactive.persister.entity.impl.ReactiveMergeCoordinatorStandardScopeFactory
 */
public class ReactiveMergeCoordinator extends ReactiveUpdateCoordinatorStandard {
	public ReactiveMergeCoordinator(
			EntityPersister entityPersister,
			SessionFactoryImplementor factory,
			MutationOperationGroup staticUpdateGroup,
			BatchKey batchKey,
			MutationOperationGroup versionUpdateGroup,
			BatchKey versionUpdateBatchkey) {
		super( entityPersister, factory, staticUpdateGroup, batchKey, versionUpdateGroup, versionUpdateBatchkey );
	}

	@Override
	protected <O extends MutationOperation> AbstractTableUpdateBuilder<O> newTableUpdateBuilder(EntityTableMapping tableMapping) {
		return new TableMergeBuilder<>( entityPersister(), tableMapping, factory() );
	}
}
