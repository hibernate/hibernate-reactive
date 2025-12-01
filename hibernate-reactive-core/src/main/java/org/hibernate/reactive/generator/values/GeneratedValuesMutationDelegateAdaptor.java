/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
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
import org.hibernate.generator.values.GeneratedValuesMutationDelegate;
import org.hibernate.jdbc.Expectation;
import org.hibernate.sql.model.ast.builder.TableMutationBuilder;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMappingProducer;

/**
 * @deprecated No longer used
 */
@Deprecated(since = "7.1",  forRemoval = true)
public class GeneratedValuesMutationDelegateAdaptor implements ReactiveGeneratedValuesMutationDelegate {
	private final ReactiveGeneratedValuesMutationDelegate delegate;

	public GeneratedValuesMutationDelegateAdaptor(GeneratedValuesMutationDelegate delegate) {
		this.delegate = (ReactiveGeneratedValuesMutationDelegate) delegate;
	}

	@Override
	public CompletionStage<GeneratedValues> reactivePerformMutation(
			PreparedStatementDetails singleStatementDetails,
			JdbcValueBindings jdbcValueBindings,
			Object modelReference,
			SharedSessionContractImplementor session) {
		return delegate.reactivePerformMutation( singleStatementDetails, jdbcValueBindings, modelReference, session );
	}

	@Override
	public TableMutationBuilder<?> createTableMutationBuilder(
			Expectation expectation,
			SessionFactoryImplementor sessionFactory) {
		return delegate.createTableMutationBuilder( expectation, sessionFactory );
	}

	@Override
	public PreparedStatement prepareStatement(String sql, SharedSessionContractImplementor session) {
		return delegate.prepareStatement( sql, session );
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
}
