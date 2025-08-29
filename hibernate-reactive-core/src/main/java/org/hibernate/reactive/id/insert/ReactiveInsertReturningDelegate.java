/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.insert;

import java.lang.invoke.MethodHandles;
import java.sql.PreparedStatement;
import java.util.List;

import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MySQLDialect;
import org.hibernate.dialect.SQLServerDialect;
import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.EventType;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.generator.values.internal.TableUpdateReturningBuilder;
import org.hibernate.id.insert.InsertReturningDelegate;
import org.hibernate.id.insert.TableInsertReturningBuilder;
import org.hibernate.jdbc.Expectation;
import org.hibernate.metamodel.mapping.BasicEntityIdentifierMapping;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.sql.model.ast.MutatingTableReference;
import org.hibernate.sql.model.ast.builder.TableMutationBuilder;

import static java.sql.Statement.NO_GENERATED_KEYS;
import static org.hibernate.generator.EventType.INSERT;

/**
 * @see InsertReturningDelegate
 */
public class ReactiveInsertReturningDelegate extends ReactiveAbstractReturningDelegate {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final MutatingTableReference tableReference;

	public ReactiveInsertReturningDelegate(EntityPersister persister, EventType timing) {
		this( persister, timing, true );
	}

	public ReactiveInsertReturningDelegate(EntityPersister persister, Dialect dialect) {
		// With JDBC it's possible to enabled GetGeneratedKeys for identity generation.
		// Vert.x doesn't have this option, so we always use the same strategy for all database.
		// But MySQL requires setting supportsArbitraryValues to false or it's not going to work.
		this( persister, INSERT, supportsArbitraryValues( dialect ) );
	}

	private static boolean supportsArbitraryValues( Dialect dialect) {
		return !( dialect instanceof MySQLDialect );
	}

	private ReactiveInsertReturningDelegate(EntityPersister persister, EventType timing, boolean supportsArbitraryValues) {
		super(
				persister,
				timing,
				supportsArbitraryValues,
				persister.getFactory().getJdbcServices().getDialect().supportsInsertReturningRowId()
		);
		this.tableReference = new MutatingTableReference( persister.getIdentifierTableMapping() );
	}

	@Override
	public TableMutationBuilder<?> createTableMutationBuilder(
			Expectation expectation,
			SessionFactoryImplementor sessionFactory) {
		if ( getTiming() == INSERT ) {
			return new TableInsertReturningBuilder( getPersister(), tableReference, getGeneratedColumns(), sessionFactory );
		}
		else {
			return new TableUpdateReturningBuilder( getPersister(), tableReference, getGeneratedColumns(), sessionFactory );
		}
	}

	@Override
	public String prepareIdentifierGeneratingInsert(String insertSQL) {
		return dialect().getIdentityColumnSupport().appendIdentitySelectToInsert(
				( (BasicEntityIdentifierMapping) getPersister().getRootEntityDescriptor().getIdentifierMapping() ).getSelectionExpression(),
				insertSQL
		);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, SharedSessionContractImplementor session) {
		return session.getJdbcCoordinator().getMutationStatementPreparer().prepareStatement( sql, NO_GENERATED_KEYS );
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
	protected GeneratedValues executeAndExtractReturning(String sql, PreparedStatement preparedStatement, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "reactiveExecuteAndExtractReturning" );
	}

	@Override
	public String createSqlString(String sql, List<String> columnNames, Dialect dialect) {
		if ( dialect instanceof SQLServerDialect ) {
			final String sqlEnd = " returning " + columnNames.get( 0 );
			int index = sql.lastIndexOf( sqlEnd );
			// FIXME: this is a hack for HHH-16365
			if ( index > -1 ) {
				sql = sql.substring( 0, index );
			}
			if ( sql.endsWith( "default values" ) ) {
				sql = sql.substring( 0, sql.indexOf( "default values" ) );
				sql = sql + "output " + getSQLServerDialectInserted( columnNames ) + " default values";
			}
			else {
				sql = sql.replace( ") values (", ") output " + getSQLServerDialectInserted( columnNames ) + " values (" );
			}
		}
		return sql;
	}

	private static String getSQLServerDialectInserted(List<String> generatedColumNames) {
		String sql = "";
		for(int i = 0; i < generatedColumNames.size(); i++) {
			if(i == generatedColumNames.size()-1 ){
				sql += "inserted." + generatedColumNames.get( i );
			}
			else {
				sql += "inserted." + generatedColumNames.get( i ) + ", ";
			}
		}
		return sql;
	}
}
