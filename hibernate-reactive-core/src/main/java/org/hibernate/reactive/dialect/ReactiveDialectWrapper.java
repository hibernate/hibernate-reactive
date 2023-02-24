/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.dialect;

import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.DialectDelegateWrapper;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.dialect.identity.IdentityColumnSupport;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableInsertStrategy;
import org.hibernate.query.sqm.mutation.spi.SqmMultiTableMutationStrategy;
import org.hibernate.reactive.dialect.identity.ReactiveIdentityColumnSupportAdapter;
import org.hibernate.reactive.query.sqm.mutation.internal.cte.ReactiveCteInsertStrategy;
import org.hibernate.reactive.query.sqm.mutation.internal.cte.ReactiveCteMutationStrategy;
import org.hibernate.reactive.sql.ast.spi.ReactivePostgreSQLSqlAstTranslator;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.SqlAstTranslatorFactory;
import org.hibernate.sql.ast.spi.StandardSqlAstTranslatorFactory;
import org.hibernate.sql.ast.tree.Statement;
import org.hibernate.sql.exec.spi.JdbcOperation;

/**
 * Wraps the given dialect to make some internal components reactive;
 * also, potentially applies a SQL syntax workaround if the wrapped Dialect
 * is extending PostgreSQLDialect.
 */
public final class ReactiveDialectWrapper extends DialectDelegateWrapper {

	//FIXME remove PostgreSQLDialect specific workarounds after HHH-16229
	private final boolean requiresPostgreSQLSyntaxProcessing;

	public ReactiveDialectWrapper(Dialect wrapped) {
		super( wrapped );
		this.requiresPostgreSQLSyntaxProcessing = ( wrapped instanceof PostgreSQLDialect );
	}

	@Override
	public SqlAstTranslatorFactory getSqlAstTranslatorFactory() {
		if ( requiresPostgreSQLSyntaxProcessing ) {
			return new StandardSqlAstTranslatorFactory() {
				@Override
				protected <T extends JdbcOperation> SqlAstTranslator<T> buildTranslator(SessionFactoryImplementor sessionFactory, Statement statement) {
					return new ReactivePostgreSQLSqlAstTranslator<>( sessionFactory, statement );
				}
			};
		}
		else {
			return wrapped.getSqlAstTranslatorFactory();
		}
	}

	@Override
	public IdentityColumnSupport getIdentityColumnSupport() {
		return new ReactiveIdentityColumnSupportAdapter( super.getIdentityColumnSupport() );
	}

	@Override
	public SqmMultiTableMutationStrategy getFallbackSqmMutationStrategy(EntityMappingType rootEntityDescriptor, RuntimeModelCreationContext runtimeModelCreationContext) {
		return new ReactiveCteMutationStrategy( rootEntityDescriptor, runtimeModelCreationContext );
	}

	@Override
	public SqmMultiTableInsertStrategy getFallbackSqmInsertStrategy(EntityMappingType rootEntityDescriptor, RuntimeModelCreationContext runtimeModelCreationContext) {
		return new ReactiveCteInsertStrategy( rootEntityDescriptor, runtimeModelCreationContext );
	}

}
