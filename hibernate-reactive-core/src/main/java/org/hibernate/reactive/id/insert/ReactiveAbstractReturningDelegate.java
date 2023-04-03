/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.insert;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.dialect.CockroachDialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MySQLDialect;
import org.hibernate.dialect.SQLServerDialect;
import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.PostInsertIdentityPersister;
import org.hibernate.id.insert.Binder;
import org.hibernate.reactive.adaptor.impl.PrepareStatementDetailsAdaptor;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.type.Type;

public interface ReactiveAbstractReturningDelegate extends ReactiveInsertGeneratedIdentifierDelegate {

	Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	PostInsertIdentityPersister getPersister();

	@Override
	default CompletionStage<Object> reactivePerformInsert(PreparedStatementDetails insertStatementDetails, JdbcValueBindings jdbcValueBindings, Object entity, SharedSessionContractImplementor session) {
		final Class<?> idType = getPersister().getIdentifierType().getReturnedClass();
		final JdbcServices jdbcServices = session.getJdbcServices();
		final String identifierColumnName = getPersister().getIdentifierColumnNames()[0];
		final String insertSql = createInsert( insertStatementDetails, identifierColumnName, jdbcServices.getDialect() );

		Object[] params = PreparedStatementAdaptor.bind( statement -> {
			PreparedStatementDetails details = new PrepareStatementDetailsAdaptor( insertStatementDetails, statement, jdbcServices );
			jdbcValueBindings.beforeStatement( details );
		} );

		ReactiveConnection reactiveConnection = ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
		return reactiveConnection
				.insertAndSelectIdentifier( insertSql, params, idType, identifierColumnName )
				.thenApply( this::validateGeneratedIdentityId );
	}

	private Object validateGeneratedIdentityId(Object generatedId) {
		if ( generatedId == null ) {
			throw LOG.noNativelyGeneratedValueReturned();
		}

		// CockroachDB might generate an identifier that fits an integer (and maybe a short) from time to time.
		// Users should not rely on it, or they might have random, hard to debug failures.
		Type identifierType = getPersister().getIdentifierType();
		if ( ( identifierType.getReturnedClass().equals( Short.class ) || identifierType.getReturnedClass().equals( Integer.class ) )
				&& getPersister().getFactory().getJdbcServices().getDialect() instanceof CockroachDialect ) {
			throw LOG.invalidIdentifierTypeForCockroachDB( identifierType.getReturnedClass(), getPersister().getEntityName() );
		}
		return generatedId;
	}

	private static String createInsert(PreparedStatementDetails insertStatementDetails, String identifierColumnName, Dialect dialect) {
		final String sqlEnd = " returning " + identifierColumnName;
		if ( dialect instanceof MySQLDialect ) {
			// For some reasons ORM generates a query with an invalid syntax
			String sql = insertStatementDetails.getSqlString();
			int index = sql.lastIndexOf( sqlEnd );
			return index > -1
					? sql.substring( 0, index )
					: sql;
		}
		if ( dialect instanceof SQLServerDialect ) {
			String sql = insertStatementDetails.getSqlString();
			int index = sql.lastIndexOf( sqlEnd );
			// FIXME: this is a hack for HHH-16365
			if ( index > -1 ) {
				sql = sql.substring( 0, index );
			}
			if ( sql.endsWith( "default values" ) ) {
				index = sql.indexOf( "default values" );
				sql = sql.substring( 0, index );
				sql = sql + "output inserted." + identifierColumnName + " default values";
			}
			else {
				sql = sql.replace( ") values (", ") output inserted." + identifierColumnName + " values (" );
			}
			return sql;
		}
		return insertStatementDetails.getSqlString();
	}

	@Override
	default CompletionStage<Object> reactivePerformInsert(String insertSQL, SharedSessionContractImplementor session, Binder binder) {
		throw LOG.notYetImplemented();
	}

}
