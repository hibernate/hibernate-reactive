/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.mutation;

import java.util.List;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.sql.model.MutationOperation;
import org.hibernate.sql.model.MutationTarget;
import org.hibernate.sql.model.TableMapping;
import org.hibernate.sql.model.ast.ColumnValueBinding;
import org.hibernate.sql.model.ast.MutatingTableReference;
import org.hibernate.sql.model.ast.RestrictedTableMutation;
import org.hibernate.sql.model.ast.builder.AbstractTableUpdateBuilder;
import org.hibernate.sql.model.internal.OptionalTableUpdate;
import org.hibernate.sql.model.internal.TableUpdateNoSet;

public class ReactiveTableMergeBuilder <O extends MutationOperation> extends AbstractTableUpdateBuilder<O> {

	public ReactiveTableMergeBuilder(
			MutationTarget<?> mutationTarget,
			TableMapping tableMapping,
			SessionFactoryImplementor sessionFactory) {
		super( mutationTarget, tableMapping, sessionFactory );
	}

	public ReactiveTableMergeBuilder(
			MutationTarget<?> mutationTarget,
			MutatingTableReference tableReference,
			SessionFactoryImplementor sessionFactory) {
		super( mutationTarget, tableReference, sessionFactory );
	}

	@SuppressWarnings("unchecked")
	@Override
	public RestrictedTableMutation<O> buildMutation() {
		final List<ColumnValueBinding> valueBindings = combine( getValueBindings(), getKeyBindings(), getLobValueBindings() );
		if ( valueBindings.isEmpty() ) {
			return (RestrictedTableMutation<O>) new TableUpdateNoSet( getMutatingTable(), getMutationTarget() );
		}

		// TODO: add getMergeDetails() (from ORM)
//		if ( getMutatingTable().getTableMapping().getUpdateDetails().getCustomSql() != null ) {
//			return (RestrictedTableMutation<O>) new TableUpdateCustomSql(
//					getMutatingTable(),
//					getMutationTarget(),
//					getSqlComment(),
//					valueBindings,
//					getKeyRestrictionBindings(),
//					getOptimisticLockBindings()
//			);
//		}

		return (RestrictedTableMutation<O>) new OptionalTableUpdate(
				getMutatingTable(),
				getMutationTarget(),
				valueBindings,
				getKeyRestrictionBindings(),
				getOptimisticLockBindings()
		);
	}
}
