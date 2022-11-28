/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.ast.spi;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.SqlAstTranslatorFactory;
import org.hibernate.sql.ast.spi.StandardSqlAstTranslatorFactory;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.spi.JdbcSelect;

public class ReactiveStandardSqlAstTranslatorFactory extends StandardSqlAstTranslatorFactory {

	private final SqlAstTranslatorFactory delegate;

	public ReactiveStandardSqlAstTranslatorFactory(SqlAstTranslatorFactory delegate) {
		this.delegate = delegate;
	}

	@Override
	public SqlAstTranslator<JdbcSelect> buildSelectTranslator(SessionFactoryImplementor sessionFactory, SelectStatement statement) {
		return new ReactiveSqlAstTranslator<>( sessionFactory, statement );

	}
}
