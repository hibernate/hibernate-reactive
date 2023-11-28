/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.dialect;

import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.DialectDelegateWrapper;
import org.hibernate.dialect.DmlTargetColumnQualifierSupport;
import org.hibernate.dialect.OracleSqlAstTranslator;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.persister.entity.mutation.EntityTableMapping;
import org.hibernate.reactive.sql.model.ReactiveDeleteOrUpsertOperation;
import org.hibernate.sql.ast.Clause;
import org.hibernate.sql.ast.tree.Statement;
import org.hibernate.sql.exec.spi.JdbcOperation;
import org.hibernate.sql.model.MutationOperation;
import org.hibernate.sql.model.internal.OptionalTableUpdate;
import org.hibernate.sql.model.jdbc.UpsertOperation;

public class ReactiveOracleSqlAstTranslator<T extends JdbcOperation> extends OracleSqlAstTranslator<T> {
	public ReactiveOracleSqlAstTranslator(
			SessionFactoryImplementor sessionFactory,
			Statement statement) {
		super( sessionFactory, statement );
	}

	@Override
	public MutationOperation createMergeOperation(OptionalTableUpdate optionalTableUpdate) {
		renderUpsertStatement( optionalTableUpdate );

		final UpsertOperation upsertOperation = new UpsertOperation(
				optionalTableUpdate.getMutatingTable().getTableMapping(),
				optionalTableUpdate.getMutationTarget(),
				getSql(),
				getParameterBinders()
		);

		return new ReactiveDeleteOrUpsertOperation(
				optionalTableUpdate.getMutationTarget(),
				(EntityTableMapping) optionalTableUpdate.getMutatingTable().getTableMapping(),
				upsertOperation,
				optionalTableUpdate
		);
	}

	// FIXME: Copy and paste from ORM
	private void renderUpsertStatement(OptionalTableUpdate optionalTableUpdate) {
		// template:
		//
		// merge into [table] as t
		// using values([bindings]) as s ([column-names])
		// on t.[key] = s.[key]
		// when not matched
		// 		then insert ...
		// when matched
		//		then update ...

		renderMergeInto( optionalTableUpdate );
		appendSql( " " );
		renderMergeUsing( optionalTableUpdate );
		appendSql( " " );
		renderMergeOn( optionalTableUpdate );
		appendSql( " " );
		renderMergeInsert( optionalTableUpdate );
		appendSql( " " );
		renderMergeUpdate( optionalTableUpdate );
	}

	@Override
	protected boolean rendersTableReferenceAlias(Clause clause) {
		// todo (6.0) : For now we just skip the alias rendering in the delete and update clauses
		//  We need some dialect support if we want to support joins in delete and update statements
		switch ( clause ) {
			case DELETE:
			case UPDATE: {
				final Dialect realDialect = DialectDelegateWrapper.extractRealDialect( getDialect() );
				return realDialect.getDmlTargetColumnQualifierSupport() == DmlTargetColumnQualifierSupport.TABLE_ALIAS;
			}
			default:
				return true;
		}
	}
}
