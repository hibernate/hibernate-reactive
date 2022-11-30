/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.ast.spi;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.SqlAstTranslatorFactory;
import org.hibernate.sql.ast.tree.delete.DeleteStatement;
import org.hibernate.sql.ast.tree.insert.InsertStatement;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.ast.tree.update.UpdateStatement;
import org.hibernate.sql.exec.spi.JdbcOperationQueryDelete;
import org.hibernate.sql.exec.spi.JdbcOperationQueryInsert;
import org.hibernate.sql.exec.spi.JdbcOperationQuerySelect;
import org.hibernate.sql.exec.spi.JdbcOperationQueryUpdate;
import org.hibernate.sql.model.ast.TableMutation;
import org.hibernate.sql.model.jdbc.JdbcMutationOperation;

public class ReactiveStandardSqlAstTranslatorFactory implements SqlAstTranslatorFactory {

	private final SqlAstTranslatorFactory delegate;

	public ReactiveStandardSqlAstTranslatorFactory(SqlAstTranslatorFactory delegate) {
		this.delegate = delegate;
	}

	@Override
	public SqlAstTranslator<JdbcOperationQuerySelect> buildSelectTranslator(
			SessionFactoryImplementor sessionFactory,
			SelectStatement statement) {
		return new ReactiveSqlAstTranslator<>( delegate.buildSelectTranslator( sessionFactory, statement ) );
	}

	@Override
	public SqlAstTranslator<JdbcOperationQueryDelete> buildDeleteTranslator(
			SessionFactoryImplementor sessionFactory,
			DeleteStatement statement) {
		return delegate.buildDeleteTranslator( sessionFactory, statement );
	}

	@Override
	public SqlAstTranslator<JdbcOperationQueryInsert> buildInsertTranslator(
			SessionFactoryImplementor sessionFactory,
			InsertStatement statement) {
		return delegate.buildInsertTranslator( sessionFactory, statement );
	}

	@Override
	public SqlAstTranslator<JdbcOperationQueryUpdate> buildUpdateTranslator(
			SessionFactoryImplementor sessionFactory,
			UpdateStatement statement) {
		return delegate.buildUpdateTranslator( sessionFactory, statement );
	}

	@Override
	public <O extends JdbcMutationOperation> SqlAstTranslator<O> buildModelMutationTranslator(
			TableMutation<O> mutation,
			SessionFactoryImplementor sessionFactory) {
		return delegate.buildModelMutationTranslator( mutation, sessionFactory );
	}
}
