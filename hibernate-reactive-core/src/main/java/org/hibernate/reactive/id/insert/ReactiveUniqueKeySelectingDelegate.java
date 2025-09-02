/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.insert;

import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.EventType;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.id.insert.UniqueKeySelectingDelegate;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.adaptor.impl.PrepareStatementDetailsAdaptor;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.generator.values.ReactiveGeneratedValuesMutationDelegate;
import org.hibernate.reactive.generator.values.internal.ReactiveGeneratedValuesHelper;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.type.Type;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

/**
 * @see UniqueKeySelectingDelegate
 */
public class ReactiveUniqueKeySelectingDelegate extends UniqueKeySelectingDelegate implements
		ReactiveGeneratedValuesMutationDelegate {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final String[] uniqueKeyPropertyNames;
	private final Type[] uniqueKeyTypes;

	public ReactiveUniqueKeySelectingDelegate(
			EntityPersister persister,
			String[] uniqueKeyPropertyNames,
			EventType timing) {
		super( persister, uniqueKeyPropertyNames, timing );
		this.uniqueKeyPropertyNames = uniqueKeyPropertyNames;
		uniqueKeyTypes = new Type[ uniqueKeyPropertyNames.length ];
		for ( int i = 0; i < uniqueKeyPropertyNames.length; i++ ) {
			uniqueKeyTypes[i] = persister.getPropertyType( uniqueKeyPropertyNames[i] );
		}
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
		return ( (ReactiveConnectionSupplier) session ).getReactiveConnection()
				.update( statementDetails.getSqlString(), params )
				.thenCompose( unused -> getGeneratedValues( entity, session ) );
	}

	private CompletionStage<GeneratedValues> getGeneratedValues(Object entity, SharedSessionContractImplementor session) {
		return ( (ReactiveConnectionSupplier) session ).getReactiveConnection()
				.selectJdbc( getSelectSQL(), getParamValues(entity, session) )
				.thenCompose( rs -> ReactiveGeneratedValuesHelper.getGeneratedValues( rs, persister, getTiming(), session ) );
	}

	protected Object[] getParamValues(Object entity, SharedSessionContractImplementor session) {
		return PreparedStatementAdaptor
				.bind( statement -> {
					int index = 1;
					for ( int i = 0; i < uniqueKeyPropertyNames.length; i++ ) {
						uniqueKeyTypes[i].nullSafeSet(
								statement,
								persister.getPropertyValue( entity, uniqueKeyPropertyNames[i] ),
								index,
								session
						);
						index += uniqueKeyTypes[i].getColumnSpan( session.getFactory().getRuntimeMetamodels() );
					}
				} );
	}

	@Override
	public GeneratedValues performMutation(
			PreparedStatementDetails statementDetails,
			JdbcValueBindings jdbcValueBindings,
			Object entity,
			SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "reactivePerformMutation" );
	}
}
