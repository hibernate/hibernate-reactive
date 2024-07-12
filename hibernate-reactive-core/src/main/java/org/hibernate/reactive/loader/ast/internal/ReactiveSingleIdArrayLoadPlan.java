/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.util.concurrent.CompletionStage;

import org.hibernate.LockOptions;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.ModelPart;
import org.hibernate.reactive.sql.results.internal.ReactiveRowTransformerArrayImpl;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.spi.JdbcParametersList;
import org.hibernate.sql.results.spi.RowTransformer;

/**
 * A reactive load plan for loading an array of state by a single restrictive part.
 *
 * @see org.hibernate.loader.ast.internal.SingleIdArrayLoadPlan
 */
public class ReactiveSingleIdArrayLoadPlan extends ReactiveSingleIdLoadPlan<Object[]> {

	public ReactiveSingleIdArrayLoadPlan(
			EntityMappingType entityMappingType,
			ModelPart restrictivePart,
			SelectStatement sqlAst,
			JdbcParametersList jdbcParameters,
			LockOptions lockOptions,
			SessionFactoryImplementor sessionFactory) {
		super( entityMappingType, restrictivePart, sqlAst, jdbcParameters, lockOptions, sessionFactory );
	}

	@Override
	protected RowTransformer<CompletionStage<Object[]>> getRowTransformer() {
		return ReactiveRowTransformerArrayImpl.asRowTransformer();
	}
}
