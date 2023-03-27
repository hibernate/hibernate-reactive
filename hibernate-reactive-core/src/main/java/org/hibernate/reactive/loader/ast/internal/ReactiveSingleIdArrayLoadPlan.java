/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.LockOptions;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metamodel.mapping.ModelPart;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.results.internal.RowTransformerDatabaseSnapshotImpl;
import org.hibernate.sql.results.spi.RowTransformer;

/**
 * A reactive load plan for loading an array of state by a single restrictive part.
 */
public class ReactiveSingleIdArrayLoadPlan extends ReactiveSingleIdLoadPlan<Object[]> {

	public ReactiveSingleIdArrayLoadPlan(
			ModelPart restrictivePart,
			SelectStatement sqlAst,
			List<JdbcParameter> jdbcParameters,
			LockOptions lockOptions,
			SessionFactoryImplementor sessionFactory) {
		super( null, restrictivePart, sqlAst, jdbcParameters, lockOptions, sessionFactory );
	}

	@Override
	protected RowTransformer<CompletionStage<Object[]>> getRowTransformer() {
		return RowTransformerDatabaseSnapshotImpl.instance();
	}

}
