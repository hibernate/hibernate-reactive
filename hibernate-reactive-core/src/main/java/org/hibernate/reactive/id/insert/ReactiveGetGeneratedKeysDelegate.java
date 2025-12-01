/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.insert;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.EventType;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.jdbc.Expectation;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.sql.model.ast.builder.TableInsertBuilderStandard;
import org.hibernate.sql.model.ast.builder.TableMutationBuilder;
import org.hibernate.sql.model.ast.builder.TableUpdateBuilderStandard;

import java.lang.invoke.MethodHandles;
import java.sql.PreparedStatement;

import static java.sql.Statement.NO_GENERATED_KEYS;
import static org.hibernate.generator.EventType.INSERT;

/**
 * ReactiveGetGeneratedKeysDelegate is used for Oracle and MySQL.
 * These 2 dbs don't support insert returning but it's still possible to have access
 * to these values (for MySQL this is true only for one generated property).
 */
public class ReactiveGetGeneratedKeysDelegate extends ReactiveAbstractReturningDelegate {
	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveGetGeneratedKeysDelegate(
			EntityPersister persister,
			boolean inferredKeys,
			EventType timing) {
		super( persister, timing, !inferredKeys, false );
	}

	@Override
	protected GeneratedValues executeAndExtractReturning(
			String sql,
			PreparedStatement preparedStatement,
			SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "reactiveExecuteAndExtractReturning" );
	}

	@Override
	public PreparedStatement prepareStatement(String sql, SharedSessionContractImplementor session) {
		return session.getJdbcCoordinator().getMutationStatementPreparer().prepareStatement( sql, NO_GENERATED_KEYS );
	}

	@Override
	public TableMutationBuilder<?> createTableMutationBuilder(
			Expectation expectation,
			SessionFactoryImplementor sessionFactory) {
		final var identifierTableMapping = getPersister().getIdentifierTableMapping();
		if ( getTiming() == INSERT ) {
			return new TableInsertBuilderStandard( getPersister(), identifierTableMapping, sessionFactory );
		}
		else {
			return new TableUpdateBuilderStandard( getPersister(), identifierTableMapping, sessionFactory );
		}
	}
}
