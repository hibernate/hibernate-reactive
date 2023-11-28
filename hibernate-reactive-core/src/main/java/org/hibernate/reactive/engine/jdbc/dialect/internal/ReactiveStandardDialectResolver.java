/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.dialect.internal;

import org.hibernate.dialect.CockroachDialect;
import org.hibernate.dialect.Database;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.DialectDelegateWrapper;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolver;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.persister.entity.mutation.EntityMutationTarget;
import org.hibernate.reactive.dialect.ReactiveOracleSqlAstTranslator;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.SqlAstTranslatorFactory;
import org.hibernate.sql.ast.spi.StandardSqlAstTranslatorFactory;
import org.hibernate.sql.ast.tree.Statement;
import org.hibernate.sql.exec.spi.JdbcOperation;
import org.hibernate.sql.model.MutationOperation;
import org.hibernate.sql.model.internal.OptionalTableUpdate;

import static org.hibernate.dialect.CockroachDialect.parseVersion;

public class ReactiveStandardDialectResolver implements DialectResolver {

	@Override
	public Dialect resolveDialect(DialectResolutionInfo info) {
		// Hibernate ORM runs an extra query to recognize CockroachDB from PostgreSQL
		// We've already done it, so we are trying to skip that step
		if ( info.getDatabaseName().startsWith( "Cockroach" ) ) {
			return new CockroachDialect( parseVersion( info.getDatabaseVersion() ) );
		}

		for ( Database database : Database.values() ) {
			if ( database.matchesResolutionInfo( info ) ) {
				Dialect dialect = database.createDialect( info );
				if ( info.getDatabaseName().toUpperCase().startsWith( "ORACLE" ) ) {
					return new DialectDelegateWrapper( dialect ) {
						@Override
						public MutationOperation createOptionalTableUpdateOperation(
								EntityMutationTarget mutationTarget,
								OptionalTableUpdate optionalTableUpdate,
								SessionFactoryImplementor factory) {
							return new ReactiveOracleSqlAstTranslator<>( factory, optionalTableUpdate )
									.createMergeOperation( optionalTableUpdate );
						}

						@Override
						public SqlAstTranslatorFactory getSqlAstTranslatorFactory() {
							return new StandardSqlAstTranslatorFactory() {
								@Override
								protected <T extends JdbcOperation> SqlAstTranslator<T> buildTranslator(SessionFactoryImplementor sessionFactory, Statement statement) {
									return new ReactiveOracleSqlAstTranslator<>( sessionFactory, statement );
								}
							};
						}
					};
				}
				return dialect;
			}
		}

		return null;
	}
}
