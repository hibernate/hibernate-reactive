/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import java.util.Collections;
import java.util.concurrent.CompletionStage;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.dialect.CockroachDialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.OracleDialect;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.dialect.SQLServerDialect;
import org.hibernate.engine.config.spi.ConfigurationService;
import org.hibernate.engine.config.spi.StandardConverters;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.id.enhanced.SequenceStyleGenerator;
import org.hibernate.id.enhanced.TableGenerator;
import org.hibernate.id.enhanced.TableStructure;
import org.hibernate.jdbc.TooManyRowsAffectedException;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.service.ServiceRegistry;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;

/**
 * Support for JPA's {@link jakarta.persistence.TableGenerator}.
 * Persistence is managed via a table which may hold multiple
 * rows distinguished by a "segment" column value.
 * <p>
 * This implementation supports block allocation, but does not
 * guarantee that generated identifiers are sequential.
 */
public class TableReactiveIdentifierGenerator extends BlockingIdentifierGenerator implements IdentifierGenerator {

	private final boolean storeLastUsedValue;

	private final String renderedTableName;
	private final String segmentColumnName;
	private final String valueColumnName;

	private final String segmentValue;
	private final long initialValue;
	private final int increment;

	private final String selectQuery;
	private final String insertQuery;
	private final String updateQuery;

	public TableReactiveIdentifierGenerator(
			TableGenerator generator,
			RuntimeModelCreationContext runtimeModelCreationContext) {
		ServiceRegistry serviceRegistry = runtimeModelCreationContext.getServiceRegistry();
		segmentColumnName = generator.getSegmentColumnName();
		valueColumnName = generator.getValueColumnName();
		segmentValue = generator.getSegmentValue();
		initialValue = generator.getInitialValue();
		increment = generator.getIncrementSize();
		storeLastUsedValue = determineStoreLastUsedValue( serviceRegistry );
		renderedTableName = generator.getTableName();

		JdbcEnvironment jdbcEnvironment = serviceRegistry.getService( JdbcEnvironment.class );
		Dialect dialect = jdbcEnvironment.getDialect();
		selectQuery = applyLocksToSelect( dialect, "tbl", buildSelectQuery( dialect ) );
		updateQuery = buildUpdateQuery( dialect );
		insertQuery = buildInsertQuery( dialect );
	}

	public TableReactiveIdentifierGenerator(
			SequenceStyleGenerator generator,
			RuntimeModelCreationContext runtimeModelCreationContext) {
		ServiceRegistry serviceRegistry = runtimeModelCreationContext.getServiceRegistry();
		JdbcEnvironment jdbcEnvironment = serviceRegistry.getService( JdbcEnvironment.class );
		Dialect dialect = jdbcEnvironment.getDialect();
		TableStructure structure = (TableStructure) generator.getDatabaseStructure();

		valueColumnName = structure.getLogicalValueColumnNameIdentifier().render( dialect );
		initialValue = structure.getInitialValue();
		increment = structure.getIncrementSize();
		storeLastUsedValue = determineStoreLastUsedValue( serviceRegistry );
		renderedTableName = structure.getPhysicalName().render();
		segmentColumnName = null;
		segmentValue = null;

		selectQuery = applyLocksToSelect( dialect, "tbl", buildSelectQuery( dialect ) );
		updateQuery = buildUpdateQuery( dialect );
		insertQuery = buildInsertQuery( dialect );
	}

	@Override
	protected int getBlockSize() {
		return increment;
	}

	@Override
	protected CompletionStage<Long> nextHiValue(ReactiveConnectionSupplier session) {
		// We need to read the current hi value from the table
		// and update it by the specified increment, but we
		// need to do it atomically, and without depending on
		// transaction rollback.
		final ReactiveConnection connection = session.getReactiveConnection();
		// 1) select the current hi value
		return connection
				.selectIdentifier( selectQuery, selectParameters(), Long.class )
				// 2) attempt to update the hi value
				.thenCompose( result -> {
					Object[] params;
					String sql;
					long id;
					if ( result == null ) {
						// if there is no row in the table, insert one
						// TODO: This not threadsafe, and can result in
						// multiple rows being inserted simultaneously.
						// It might be better to just throw an exception
						// here, and require that the table was populated
						// when it was created
						id = initialValue;
						long insertedValue = storeLastUsedValue ? id - increment : id;
						params = insertParameters( insertedValue );
						sql = insertQuery;
					}
					else {
						// otherwise, update the existing row
						long currentValue = result;
						long updatedValue = currentValue + increment;
						id = storeLastUsedValue ? updatedValue : currentValue;
						params = updateParameters( currentValue, updatedValue );
						sql = updateQuery;
					}
					return connection.update( sql, params )
							// 3) check the updated row count to detect simultaneous update
							.thenCompose( rowCount -> checkValue( session, rowCount, id ) );
				} );
	}

	private CompletionStage<Long> checkValue(ReactiveConnectionSupplier session, Integer rowCount, long id) {
		return switch ( rowCount ) {
			// we successfully obtained the next hi value
			case 1 -> completedFuture( id );
			// someone else grabbed the next hi value, so retry everything from scratch
			case 0 -> nextHiValue( session );
			// Something is wrong
			default -> failedFuture( new TooManyRowsAffectedException( "multiple rows in id table", 1, rowCount ) );
		};
	}

	private String applyLocksToSelect(Dialect dialect, String alias, String query) {
		return dialect.applyLocksToSql(
				query,
				new LockOptions( LockMode.PESSIMISTIC_WRITE ).setAliasSpecificLockMode( alias, LockMode.PESSIMISTIC_WRITE ),
				Collections.singletonMap( alias, new String[] { valueColumnName } )
		);
	}

	protected Boolean determineStoreLastUsedValue(ServiceRegistry serviceRegistry) {
		return serviceRegistry.getService( ConfigurationService.class )
				.getSetting( Settings.TABLE_GENERATOR_STORE_LAST_USED, StandardConverters.BOOLEAN, true );
	}

	protected Object[] updateParameters(long currentValue, long updatedValue) {
		return segmentColumnName == null
				? new Object[] { updatedValue, currentValue }
				: new Object[] { updatedValue, currentValue, segmentValue };
	}

	protected Object[] insertParameters(long insertedValue) {
		return segmentColumnName == null
				? new Object[] { insertedValue }
				: new Object[] { segmentValue, insertedValue };
	}

	protected Object[] selectParameters() {
		return segmentColumnName == null
				? new Object[] {}
				: new Object[] { segmentValue };
	}

	protected String buildSelectQuery(Dialect dialect) {
		final String sql = "select tbl." + valueColumnName + " from " + renderedTableName + " tbl";
		return segmentColumnName != null
				? sql + " where tbl." + segmentColumnName + "=" + paramMarker( dialect, 1 )
				: sql;
	}

	protected String buildUpdateQuery(Dialect dialect) {
		final String sql = "update " + renderedTableName
				+ " set " + valueColumnName + "=" + paramMarker( dialect, 1 )
				+ " where " + valueColumnName + "=" + paramMarker( dialect, 2 );
		return segmentValue != null
				? sql + " and " + segmentColumnName + "=" + paramMarker( dialect, 3 )
				: sql;
	}

	protected String buildInsertQuery(Dialect dialect) {
		if ( segmentColumnName == null ) {
			return "insert into " + renderedTableName + " (" + valueColumnName + ") values (" + paramMarker( dialect, 1 ) + ")";
		}
		return "insert into " + renderedTableName + " (" + segmentColumnName + ", " + valueColumnName + ") values ("
				+ paramMarker( dialect, 1 ) + ", " + paramMarker( dialect, 2 ) + ")";
	}

	private static String paramMarker(Dialect dialect, int pos) {
		if ( dialect instanceof PostgreSQLDialect || dialect instanceof CockroachDialect ) {
			return "$" + pos;
		}
		if ( dialect instanceof SQLServerDialect ) {
			return "@P" + pos;
		}
		if ( dialect instanceof OracleDialect ) {
			return ":" + pos;
		}
		return "?";
	}
}
