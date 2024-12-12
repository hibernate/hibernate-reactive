/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.env.internal;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.dialect.DB2Dialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MySQLDialect;
import org.hibernate.dialect.OracleDialect;
import org.hibernate.dialect.SQLServerDialect;
import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.MutationExecutor;
import org.hibernate.engine.jdbc.mutation.OperationResultChecker;
import org.hibernate.engine.jdbc.mutation.ParameterUsage;
import org.hibernate.engine.jdbc.mutation.TableInclusionChecker;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.persister.entity.mutation.EntityTableMapping;
import org.hibernate.reactive.adaptor.impl.PrepareStatementDetailsAdaptor;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.sql.model.TableMapping;
import org.hibernate.sql.model.ValuesAnalysis;

import static org.hibernate.reactive.engine.jdbc.ResultsCheckerUtil.checkResults;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;
import static org.hibernate.reactive.util.impl.CompletionStages.nullFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER;

/**
 * @see org.hibernate.engine.jdbc.mutation.internal.AbstractMutationExecutor
 */
public interface ReactiveMutationExecutor extends MutationExecutor {

	@Override
	default GeneratedValues execute(
			Object modelReference,
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session) {
		throw make( Log.class, MethodHandles.lookup() ).nonReactiveMethodCall( "executeReactive" );
	}

	default CompletionStage<GeneratedValues> executeReactive(
			Object modelReference,
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session) {
		return executeReactive( modelReference, valuesAnalysis, inclusionChecker, resultChecker, session, true, null );
	}

	default CompletionStage<GeneratedValues> executeReactive(
			Object modelReference,
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session,
			boolean isIdentityInsert,
			String[] identifierColumnsNames) {
		return performReactiveNonBatchedOperations( modelReference, valuesAnalysis, inclusionChecker, resultChecker, session, isIdentityInsert, identifierColumnsNames )
				.thenCompose( generatedValues -> performReactiveSelfExecutingOperations( valuesAnalysis, inclusionChecker, session )
									  .thenCompose( v -> performReactiveBatchedOperations( valuesAnalysis, inclusionChecker, resultChecker, session ) )
									  .thenApply( v -> generatedValues )
				);
	}

	default CompletionStage<GeneratedValues> performReactiveNonBatchedOperations(
			Object modelReference,
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session,
			boolean isIdentityInsert,
			String[] identifierColumnsNames) {
		return nullFuture();
	}

	default CompletionStage<Void> performReactiveSelfExecutingOperations(
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker,
			SharedSessionContractImplementor session) {
		return voidFuture();
	}

	default CompletionStage<Void> performReactiveBatchedOperations(
			ValuesAnalysis valuesAnalysis,
			TableInclusionChecker inclusionChecker, OperationResultChecker resultChecker,
			SharedSessionContractImplementor session) {
		return voidFuture();
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

	/**
	 * Perform a non-batched mutation
	 */
	default CompletionStage<Void> performReactiveNonBatchedMutation(
			PreparedStatementDetails statementDetails,
			Object id,
			JdbcValueBindings valueBindings,
			TableInclusionChecker inclusionChecker,
			OperationResultChecker resultChecker,
			SharedSessionContractImplementor session,
			String[] identifierColumnsNames) {
		if ( statementDetails == null ) {
			return nullFuture();
		}

		final TableMapping tableDetails = statementDetails.getMutatingTableDetails();
		if ( inclusionChecker != null && !inclusionChecker.include( tableDetails ) ) {
			if ( MODEL_MUTATION_LOGGER.isTraceEnabled() ) {
				MODEL_MUTATION_LOGGER.tracef( "Skipping execution of secondary insert : %s", tableDetails.getTableName() );
			}
			return voidFuture();
		}

		if ( id != null ) {
			assert !tableDetails.isIdentifierTable() : "Unsupported identifier table with generated id";
			( (EntityTableMapping) tableDetails ).getKeyMapping().breakDownKeyJdbcValues(
					id,
					(jdbcValue, columnMapping) -> valueBindings.bindValue(
							jdbcValue,
							tableDetails.getTableName(),
							columnMapping.getColumnName(),
							ParameterUsage.SET
					),
					session
			);
		}

		// If we get here the statement is needed - make sure it is resolved
		Object[] params = PreparedStatementAdaptor.bind( statement -> {
			PreparedStatementDetails details = new PrepareStatementDetailsAdaptor( statementDetails, statement, session.getJdbcServices() );
			valueBindings.beforeStatement( details );
		} );

		Dialect dialect = session.getJdbcServices().getDialect();
		ReactiveConnection reactiveConnection = ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
		String sqlString = statementDetails.getSqlString();
		if ( identifierColumnsNames != null ) {
			sqlString = createInsert( statementDetails.getSqlString(), identifierColumnsNames[0], dialect );
		}
		return reactiveConnection
				.update( sqlString, params )
				.thenCompose( affectedRowCount -> {
					if ( affectedRowCount == 0 && tableDetails.isOptional() ) {
						// the optional table did not have a row
						return voidFuture();
					}
					checkResults( session, statementDetails, resultChecker, affectedRowCount, -1 );
					return voidFuture();
				} )
				.whenComplete( (o, throwable) -> {
					if ( statementDetails.getStatement() != null ) {
						statementDetails.releaseStatement( session );
					}
					valueBindings.afterStatement( tableDetails );
				} );
	}
}
