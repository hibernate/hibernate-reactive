/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.insert;

import java.lang.invoke.MethodHandles;
import java.sql.PreparedStatement;
import java.util.concurrent.CompletionStage;

import org.hibernate.dialect.CockroachDialect;
import org.hibernate.dialect.DB2Dialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MySQLDialect;
import org.hibernate.dialect.OracleDialect;
import org.hibernate.dialect.SQLServerDialect;
import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.id.insert.Binder;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.adaptor.impl.PrepareStatementDetailsAdaptor;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.type.Type;

import static org.hibernate.reactive.logging.impl.LoggerFactory.make;

public interface ReactiveAbstractReturningDelegate extends ReactiveInsertGeneratedIdentifierDelegate {

	@Override
	PreparedStatement prepareStatement(String insertSql, SharedSessionContractImplementor session);

	EntityPersister getPersister();

	@Override
	default CompletionStage<GeneratedValues> reactivePerformInsertReturning(String sql, SharedSessionContractImplementor session, Binder binder) {
		final String identifierColumnName = getPersister().getIdentifierColumnNames()[0];
		final JdbcServices jdbcServices = session.getJdbcServices();
		final String insertSql = createInsert( sql, identifierColumnName, jdbcServices.getDialect() );
		final Object[] params = PreparedStatementAdaptor.bind( binder::bindValues );
		return reactiveExecuteAndExtractReturning( insertSql, params, session )
				.thenApply( this::validateGeneratedIdentityId );
	}

	CompletionStage<GeneratedValues> reactiveExecuteAndExtractReturning(String sql, Object[] params,  SharedSessionContractImplementor session);

	@Override
	default CompletionStage<GeneratedValues> reactivePerformMutation(
			PreparedStatementDetails statementDetails,
			JdbcValueBindings valueBindings,
			Object entity,
			SharedSessionContractImplementor session) {
		Object[] params = PreparedStatementAdaptor.bind( statement -> {
			PreparedStatementDetails details = new PrepareStatementDetailsAdaptor( statementDetails, statement, session.getJdbcServices() );
			valueBindings.beforeStatement( details );
		} );
		final String identifierColumnName = getPersister().getIdentifierColumnNames()[0];
		final JdbcServices jdbcServices = session.getJdbcServices();
		final String insertSql = createInsert( statementDetails.getSqlString(), identifierColumnName, jdbcServices.getDialect() );
		return reactiveExecuteAndExtractReturning( insertSql, params, session )
				.whenComplete( (generatedValues, throwable) -> {
					if ( statementDetails.getStatement() != null ) {
						statementDetails.releaseStatement( session );
					}
					valueBindings.afterStatement( statementDetails.getMutatingTableDetails() );
				} );
	}

	default GeneratedValues validateGeneratedIdentityId(GeneratedValues generatedId) {
		if ( generatedId == null ) {
			throw make( Log.class, MethodHandles.lookup() ).noNativelyGeneratedValueReturned();
		}

		// CockroachDB might generate an identifier that fits an integer (and maybe a short) from time to time.
		// Users should not rely on it, or they might have random, hard to debug failures.
		Type identifierType = getPersister().getIdentifierType();
		if ( ( identifierType.getReturnedClass().equals( Short.class ) || identifierType.getReturnedClass().equals( Integer.class ) )
				&& getPersister().getFactory().getJdbcServices().getDialect() instanceof CockroachDialect ) {
			throw make( Log.class, MethodHandles.lookup() ).invalidIdentifierTypeForCockroachDB( identifierType.getReturnedClass(), getPersister().getEntityName() );
		}
		return generatedId;
	}

	private static String createInsert(String insertSql, String identifierColumnName, Dialect dialect) {
		String sql = insertSql;
		final String sqlEnd = " returning " + identifierColumnName;
		if ( dialect instanceof MySQLDialect ) {
			// For some reason ORM generates a query with an invalid syntax
			int index = sql.lastIndexOf( sqlEnd );
			return index > -1
					? sql.substring( 0, index )
					: sql;
		}
		if ( dialect instanceof SQLServerDialect ) {
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
		if ( dialect instanceof DB2Dialect ) {
			// ORM query: select id from new table ( insert into IntegerTypeEntity values ( ))
			// Correct  : select id from new table ( insert into LongTypeEntity (id) values (default))
			return sql.replace( " values ( ))", " (" + identifierColumnName + ") values (default))" );
		}
		if ( dialect instanceof OracleDialect ) {
			final String valuesStr = " values ( )";
			int index = sql.lastIndexOf( sqlEnd );
			// remove "returning id" since it's added via
			if ( index > -1 ) {
				sql = sql.substring( 0, index );
			}

			// Oracle is expecting values (default)
			if ( sql.endsWith( valuesStr ) ) {
				index = sql.lastIndexOf( valuesStr );
				sql = sql.substring( 0, index );
				sql = sql + " values (default)";
			}

			return sql;
		}
		return sql;
	}
}
