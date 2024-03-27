/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.insert;

import java.lang.invoke.MethodHandles;
import java.sql.PreparedStatement;
import java.util.concurrent.CompletionStage;

import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.EventType;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.id.PostInsertIdentityPersister;
import org.hibernate.id.insert.InsertReturningDelegate;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;

import static org.hibernate.reactive.generator.values.internal.ReactiveGeneratedValuesHelper.getGeneratedValues;

/**
 * @see InsertReturningDelegate
 */
public class ReactiveInsertReturningDelegate extends InsertReturningDelegate implements ReactiveAbstractReturningDelegate {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final PostInsertIdentityPersister persister;

	public ReactiveInsertReturningDelegate(EntityPersister persister, EventType timing) {
		super( persister, timing );
		this.persister = (PostInsertIdentityPersister) persister;
	}

	public ReactiveInsertReturningDelegate(PostInsertIdentityPersister persister, Dialect dialect) {
		super( persister, dialect );
		this.persister = persister;
	}

	@Override
	public PostInsertIdentityPersister getPersister() {
		return persister;
	}

	@Override
	public GeneratedValues performMutation(
			PreparedStatementDetails statementDetails,
			JdbcValueBindings valueBindings,
			Object entity,
			SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "reactivePerformMutation" );
	}

	@Override
	public CompletionStage<GeneratedValues> reactiveExecuteAndExtractReturning(String sql, Object[] params, SharedSessionContractImplementor session)  {
		final Class<?> idType = getPersister().getIdentifierType().getReturnedClass();
		final String identifierColumnName = getPersister().getIdentifierColumnNames()[0];
		return ( (ReactiveConnectionSupplier) session )
				.getReactiveConnection()
				.insertAndSelectIdentifierAsResultSet( sql, params, idType, identifierColumnName )
				.thenCompose( rs -> getGeneratedValues( rs, getPersister(), getTiming(), session ) );
	}

	@Override
	protected GeneratedValues executeAndExtractReturning(String sql, PreparedStatement preparedStatement, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "reactiveExecuteAndExtractReturning" );
	}
}
