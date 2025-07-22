/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.insert;

import org.hibernate.HibernateException;
import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.EventType;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.id.insert.AbstractSelectingDelegate;
import org.hibernate.id.insert.BasicSelectingDelegate;
import org.hibernate.jdbc.Expectation;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.adaptor.impl.PrepareStatementDetailsAdaptor;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.generator.values.internal.ReactiveGeneratedValuesHelper;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.sql.model.ast.builder.TableInsertBuilderStandard;
import org.hibernate.sql.model.ast.builder.TableMutationBuilder;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

/**
 * @see BasicSelectingDelegate
 */
public class ReactiveBasicSelectingDelegate extends AbstractSelectingDelegate implements ReactiveAbstractSelectingDelegate {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	final private EntityPersister persister;

	public ReactiveBasicSelectingDelegate(EntityPersister persister) {
		super( persister, EventType.INSERT, false, false );
		this.persister = persister;
	}

	@Override
	public CompletionStage<GeneratedValues> reactivePerformMutation(
			PreparedStatementDetails statementDetails,
			JdbcValueBindings valueBindings,
			Object entity,
			SharedSessionContractImplementor session) {
		final JdbcServices jdbcServices = session.getJdbcServices();
		final Object[] params = PreparedStatementAdaptor.bind( statement -> {
			PreparedStatementDetails details = new PrepareStatementDetailsAdaptor( statementDetails, statement, jdbcServices );
			valueBindings.beforeStatement( details );
		} );
		return ((ReactiveConnectionSupplier) session).getReactiveConnection()
				.update(  statementDetails.getSqlString(), params )
				.thenCompose( unused -> getGeneratedValues( session ) );
	}

	private CompletionStage<GeneratedValues> getGeneratedValues(SharedSessionContractImplementor session) {
		return ( (ReactiveConnectionSupplier) session ).getReactiveConnection()
				.selectJdbc( getSelectSQL() )
				.thenCompose( rs -> ReactiveGeneratedValuesHelper.getGeneratedValues( rs, persister, getTiming(), session ) );
	}

	@Override
	public TableMutationBuilder<?> createTableMutationBuilder( Expectation expectation, SessionFactoryImplementor factory) {
		return new TableInsertBuilderStandard( persister, persister.getIdentifierTableMapping(), factory );
	}

	@Override
	protected String getSelectSQL() {
		final String identitySelectString = persister.getIdentitySelectString();
		if ( identitySelectString == null
				&& !dialect().getIdentityColumnSupport().supportsInsertSelectIdentity() ) {
			throw new HibernateException( "Cannot retrieve the generated identity, because the dialect does not support selecting the last generated identity" );
		}
		return identitySelectString;
	}


	@Override
	public GeneratedValues performMutation(
			PreparedStatementDetails singleStatementDetails,
			JdbcValueBindings jdbcValueBindings,
			Object modelReference,
			SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "reactivePerformMutation" );
	}

}
