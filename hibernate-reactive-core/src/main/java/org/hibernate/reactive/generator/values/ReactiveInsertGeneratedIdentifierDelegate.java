/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.generator.values;

import java.sql.PreparedStatement;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.EventType;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.id.insert.Binder;
import org.hibernate.id.insert.InsertGeneratedIdentifierDelegate;
import org.hibernate.jdbc.Expectation;
import org.hibernate.metamodel.mapping.BasicEntityIdentifierMapping;
import org.hibernate.sql.model.ast.builder.TableInsertBuilder;
import org.hibernate.sql.model.ast.builder.TableMutationBuilder;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMappingProducer;

public class ReactiveInsertGeneratedIdentifierDelegate implements InsertGeneratedIdentifierDelegate, ReactiveGeneratedValuesMutationDelegate {
	private final InsertGeneratedIdentifierDelegate delegate;

	public ReactiveInsertGeneratedIdentifierDelegate(InsertGeneratedIdentifierDelegate delegate) {
		this.delegate = delegate;
	}

	@Override
	public TableInsertBuilder createTableInsertBuilder(
			BasicEntityIdentifierMapping identifierMapping,
			Expectation expectation,
			SessionFactoryImplementor sessionFactory) {
		return delegate.createTableInsertBuilder( identifierMapping, expectation, sessionFactory );
	}

	@Override
	public PreparedStatement prepareStatement(String insertSql, SharedSessionContractImplementor session) {
		return delegate.prepareStatement( insertSql, session );
	}

	@Override
	public Object performInsert(
			PreparedStatementDetails insertStatementDetails,
			JdbcValueBindings valueBindings,
			Object entity,
			SharedSessionContractImplementor session) {
		return delegate.performInsert( insertStatementDetails, valueBindings, entity, session );
	}

	@Override
	public String prepareIdentifierGeneratingInsert(String insertSQL) {
		return delegate.prepareIdentifierGeneratingInsert( insertSQL );
	}

	@Override
	public Object performInsert(String insertSQL, SharedSessionContractImplementor session, Binder binder) {
		return delegate.performInsert( insertSQL, session, binder );
	}

	@Override
	public GeneratedValues performInsertReturning(
			String insertSQL,
			SharedSessionContractImplementor session,
			Binder binder) {
		return delegate.performInsertReturning( insertSQL, session, binder );
	}

	@Override
	public TableMutationBuilder<?> createTableMutationBuilder(
			Expectation expectation,
			SessionFactoryImplementor sessionFactory) {
		return delegate.createTableMutationBuilder( expectation, sessionFactory );
	}

	@Override
	public GeneratedValues performMutation(
			PreparedStatementDetails statementDetails,
			JdbcValueBindings valueBindings,
			Object entity,
			SharedSessionContractImplementor session) {
		return delegate.performMutation( statementDetails, valueBindings, entity, session );
	}

	@Override
	public EventType getTiming() {
		return delegate.getTiming();
	}

	@Override
	public boolean supportsArbitraryValues() {
		return delegate.supportsArbitraryValues();
	}

	@Override
	public boolean supportsRowId() {
		return delegate.supportsRowId();
	}

	@Override
	public JdbcValuesMappingProducer getGeneratedValuesMappingProducer() {
		return delegate.getGeneratedValuesMappingProducer();
	}

	@Override
	public CompletionStage<GeneratedValues> reactivePerformMutation(
			PreparedStatementDetails singleStatementDetails,
			JdbcValueBindings jdbcValueBindings,
			Object modelReference,
			SharedSessionContractImplementor session) {
		return null;
	}
}
