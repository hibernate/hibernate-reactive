/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.dialect;

import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.dialect.PostgreSQLDriverKind;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.reactive.sql.ast.spi.ReactivePostgreSQLSqlAstTranslator;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.SqlAstTranslatorFactory;
import org.hibernate.sql.ast.spi.StandardSqlAstTranslatorFactory;
import org.hibernate.sql.ast.tree.Statement;
import org.hibernate.sql.exec.spi.JdbcOperation;

public class ReactivePostgreSQLDialect extends PostgreSQLDialect {

	public ReactivePostgreSQLDialect() {
	}

	public ReactivePostgreSQLDialect(DialectResolutionInfo info) {
		super( info );
	}

	public ReactivePostgreSQLDialect(DatabaseVersion version) {
		super( version );
	}

	public ReactivePostgreSQLDialect(DatabaseVersion version, PostgreSQLDriverKind driverKind) {
		super( version );
	}

	@Override
	public SqlAstTranslatorFactory getSqlAstTranslatorFactory() {
		return new StandardSqlAstTranslatorFactory() {
			@Override
			protected <T extends JdbcOperation> SqlAstTranslator<T> buildTranslator(SessionFactoryImplementor sessionFactory, Statement statement) {
				return new ReactivePostgreSQLSqlAstTranslator<>( sessionFactory, statement );
			}
		};
	}
}
