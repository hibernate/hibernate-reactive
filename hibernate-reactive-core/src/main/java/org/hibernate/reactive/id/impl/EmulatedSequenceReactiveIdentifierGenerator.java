/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import org.hibernate.dialect.CockroachDialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.OracleDialect;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.dialect.SQLServerDialect;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.id.enhanced.SequenceStyleGenerator;
import org.hibernate.id.enhanced.TableStructure;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.Type;

import java.util.Properties;

import static org.hibernate.internal.util.config.ConfigurationHelper.getString;

/**
 * Support for JPA's {@link jakarta.persistence.SequenceGenerator}
 * for databases which do not support sequences. Persistence is
 * managed via a table with just one row and one column.
 * <p>
 * This implementation supports block allocation, but does not
 * guarantee that generated identifiers are sequential.
 */
public class EmulatedSequenceReactiveIdentifierGenerator extends TableReactiveIdentifierGenerator {

	public EmulatedSequenceReactiveIdentifierGenerator(TableStructure structure, RuntimeModelCreationContext runtimeModelCreationContext) {
		super( structure, runtimeModelCreationContext );
	}

	@Override
	public void configure(Type type, Properties params, ServiceRegistry serviceRegistry) {
		super.configure( type, params, serviceRegistry );
		ReactiveSequenceIdentifierGenerator generator = new ReactiveSequenceIdentifierGenerator();
		generator.configure( type, params, serviceRegistry );
	}

	@Override
	protected Boolean determineStoreLastUsedValue(ServiceRegistry serviceRegistry) {
		return false;
	}

	@Override
	protected String determineTableName(Type type, Properties params, ServiceRegistry serviceRegistry) {
		ReactiveSequenceIdentifierGenerator generator = new ReactiveSequenceIdentifierGenerator();
		generator.configure( type, params, serviceRegistry );
		return generator.getSequenceName().render();
	}

	@Override
	protected String determineValueColumnNameForTable(Properties params, JdbcEnvironment jdbcEnvironment) {
		final String name = getString( SequenceStyleGenerator.VALUE_COLUMN_PARAM, params, SequenceStyleGenerator.DEF_VALUE_COLUMN );
		return jdbcEnvironment.getIdentifierHelper().toIdentifier( name ).render( jdbcEnvironment.getDialect() );
	}

	@Override
	protected String determineSegmentColumnName(Properties params, JdbcEnvironment jdbcEnvironment) {
		return null;
	}

	@Override
	protected String determineSegmentValue(Properties params) {
		return null;
	}

	@Override
	protected Object[] updateParameters(long currentValue, long updatedValue) {
		return new Object[] { updatedValue, currentValue };
	}

	@Override
	protected Object[] insertParameters(long insertedValue) {
		return new Object[] { insertedValue };
	}

	@Override
	protected Object[] selectParameters() {
		return new Object[] {};
	}

	@Override
	protected String buildSelectQuery(Dialect dialect) {
		return "select tbl." + valueColumnName + " from " + renderedTableName + " tbl";
	}

	@Override
	protected String buildUpdateQuery(Dialect dialect) {
		if ( dialect instanceof PostgreSQLDialect || dialect instanceof CockroachDialect ) {
			return "update " + renderedTableName + " set " + valueColumnName + "=$1"
					+ " where " + valueColumnName + "=$2";

		}
		if ( dialect instanceof SQLServerDialect ) {
			return "update " + renderedTableName + " set " + valueColumnName + "=@P1"
					+ " where " + valueColumnName + "=@P2";

		}
		if ( dialect instanceof OracleDialect ) {
			return "update " + renderedTableName + " set " + valueColumnName + "=:1"
					+ " where " + valueColumnName + "=:2";

		}
		return "update " + renderedTableName + " set " + valueColumnName + "=?"
				+ " where " + valueColumnName + "=?";
	}

	@Override
	protected String buildInsertQuery(Dialect dialect) {
		final String sql = "insert into " + renderedTableName + " (" + valueColumnName + ") values ";
		if ( dialect instanceof PostgreSQLDialect || dialect instanceof CockroachDialect ) {
			return sql + "($1)";
		}
		if ( dialect instanceof SQLServerDialect ) {
			return sql + "(@P1)";
		}
		if ( dialect instanceof OracleDialect ) {
			return sql + "(:1)";
		}
		return sql + "(?)";
	}

}
