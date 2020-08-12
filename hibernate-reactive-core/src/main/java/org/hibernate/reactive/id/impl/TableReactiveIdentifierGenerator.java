/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.boot.model.relational.QualifiedName;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.config.spi.ConfigurationService;
import org.hibernate.engine.config.spi.StandardConverters;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.id.Configurable;
import org.hibernate.id.enhanced.SequenceStyleGenerator;
import org.hibernate.id.enhanced.TableGenerator;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.Type;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletionStage;

import static org.hibernate.id.enhanced.TableGenerator.CONFIG_PREFER_SEGMENT_PER_ENTITY;
import static org.hibernate.id.enhanced.TableGenerator.DEF_SEGMENT_COLUMN;
import static org.hibernate.id.enhanced.TableGenerator.DEF_SEGMENT_VALUE;
import static org.hibernate.id.enhanced.TableGenerator.SEGMENT_COLUMN_PARAM;
import static org.hibernate.id.enhanced.TableGenerator.SEGMENT_VALUE_PARAM;
import static org.hibernate.id.enhanced.TableGenerator.TABLE;
import static org.hibernate.internal.util.config.ConfigurationHelper.getBoolean;
import static org.hibernate.internal.util.config.ConfigurationHelper.getInt;
import static org.hibernate.internal.util.config.ConfigurationHelper.getString;
import static org.hibernate.reactive.id.impl.IdentifierGeneration.determineSequenceName;
import static org.hibernate.reactive.id.impl.IdentifierGeneration.determineTableName;

/**
 * Support for JPA's {@link javax.persistence.TableGenerator}. This
 * generator functions in two different modes: as a table generator
 * where different logical sequences are represented by different
 * rows ("segments"), or as an emulated sequence generator with
 * just one row and one column.
 * <p>
 * This implementation supports block allocation, but does not
 * guarantee that generated identifiers are sequential.
 */
public class TableReactiveIdentifierGenerator
		implements ReactiveIdentifierGenerator<Long>, Configurable {

	private boolean storeLastUsedValue;

	private String renderedTableName;

	private String segmentColumnName;
	private String segmentValue;

	private String valueColumnName;
	private long initialValue;
	private int increment;

	private String selectQuery;
	private String insertQuery;
	private String updateQuery;

	private int loValue;
	private long hiValue;

	private final boolean sequenceEmulator;

	private synchronized long next() {
		return loValue>0 && loValue<increment
				? hiValue + loValue++
				: -1; //flag value indicating that we need to hit db
	}

	private synchronized long next(long hi) {
		hiValue = hi;
		loValue = 1;
		return hi;
	}

	@Override
	public CompletionStage<Long> generate(ReactiveConnectionSupplier session, Object entity) {
		long local = next();
		if ( local >= 0 ) {
			return CompletionStages.completedFuture(local);
		}

		Object[] param = segmentColumnName == null ? new Object[] {} : new Object[] {segmentValue};
		ReactiveConnection connection = session.getReactiveConnection();
		return connection.selectLong( selectQuery, param )
				.thenCompose( result -> {
					Object[] params;
					String sql;
					long id;
					if ( result == null ) {
						id = initialValue;
						long insertedValue = storeLastUsedValue ? id - increment : id;
						params = segmentColumnName == null ?
								new Object[] {insertedValue} :
								new Object[] {segmentValue, insertedValue};
						sql = insertQuery;
					}
					else {
						long currentValue = result;
						long updatedValue = currentValue + increment;
						id = storeLastUsedValue ? updatedValue : currentValue;
						params = segmentColumnName == null ?
								new Object[] {updatedValue, currentValue} :
								new Object[] {updatedValue, currentValue, segmentValue};
						sql = updateQuery;
					}
					return connection.update( sql, params )
							.thenCompose(
									rowCount -> rowCount==1
											//we successfully obtained the next hi value
											? CompletionStages.completedFuture( next(id) )
											//someone else grabbed the next hi value
											//so retry everything from scratch
											: generate( session, entity )
							);
				} );
	}

	TableReactiveIdentifierGenerator(boolean sequenceEmulator) {
		this.sequenceEmulator = sequenceEmulator;
	}

	@Override
	public void configure(Type type, Properties params, ServiceRegistry serviceRegistry) {
		JdbcEnvironment jdbcEnvironment = serviceRegistry.getService( JdbcEnvironment.class );
		Dialect dialect = jdbcEnvironment.getDialect();

		QualifiedName qualifiedTableName;
		if (sequenceEmulator) {
			//it's a SequenceStyleGenerator backed by a table
			qualifiedTableName = determineSequenceName( params, serviceRegistry );
			valueColumnName = determineValueColumnNameForSequenceEmulation( params, jdbcEnvironment );
			segmentColumnName = null;
			segmentValue = null;
			initialValue = determineInitialValueForSequenceEmulation( params );
			increment = determineIncrementForSequenceEmulation( params );

			storeLastUsedValue = false;
		}
		else {
			//It's a regular TableGenerator
			qualifiedTableName = determineTableName( params, serviceRegistry );
			segmentColumnName = determineSegmentColumnName( params, jdbcEnvironment );
			valueColumnName = determineValueColumnNameForTable( params, jdbcEnvironment );
			segmentValue = determineSegmentValue( params );
			initialValue = determineInitialValueForTable( params );
			increment = determineIncrementForTable( params );

			storeLastUsedValue = serviceRegistry.getService( ConfigurationService.class )
					.getSetting( Settings.TABLE_GENERATOR_STORE_LAST_USED,
							StandardConverters.BOOLEAN, true );
		}

		// allow physical naming strategies a chance to kick in
		renderedTableName = jdbcEnvironment.getQualifiedObjectNameFormatter()
				.format(qualifiedTableName, dialect );

		selectQuery = buildSelectQuery( dialect );
		updateQuery = buildUpdateQuery( dialect );
		insertQuery = buildInsertQuery( dialect );
	}

	protected String determineSegmentColumnName(Properties params, JdbcEnvironment jdbcEnvironment) {
		final String name = getString( SEGMENT_COLUMN_PARAM, params, DEF_SEGMENT_COLUMN );
		return jdbcEnvironment.getIdentifierHelper().toIdentifier( name ).render( jdbcEnvironment.getDialect() );
	}

	protected String determineValueColumnNameForTable(Properties params, JdbcEnvironment jdbcEnvironment) {
		final String name = getString( TableGenerator.VALUE_COLUMN_PARAM, params, TableGenerator.DEF_VALUE_COLUMN );
		return jdbcEnvironment.getIdentifierHelper().toIdentifier( name ).render( jdbcEnvironment.getDialect() );
	}

	static String determineValueColumnNameForSequenceEmulation(Properties params, JdbcEnvironment jdbcEnvironment) {
		final String name = getString( SequenceStyleGenerator.VALUE_COLUMN_PARAM, params, SequenceStyleGenerator.DEF_VALUE_COLUMN );
		return jdbcEnvironment.getIdentifierHelper().toIdentifier( name ).render( jdbcEnvironment.getDialect() );
	}

	protected String determineSegmentValue(Properties params) {
		String segmentValue = params.getProperty( SEGMENT_VALUE_PARAM );
		if ( StringHelper.isEmpty( segmentValue ) ) {
			segmentValue = determineDefaultSegmentValue( params );
		}
		return segmentValue;
	}

	protected String determineDefaultSegmentValue(Properties params) {
		final boolean preferSegmentPerEntity = getBoolean( CONFIG_PREFER_SEGMENT_PER_ENTITY, params, false );
		return preferSegmentPerEntity ? params.getProperty( TABLE ) : DEF_SEGMENT_VALUE;
	}

	protected int determineInitialValueForTable(Properties params) {
		return getInt( TableGenerator.INITIAL_PARAM, params, TableGenerator.DEFAULT_INITIAL_VALUE );
	}

	protected int determineInitialValueForSequenceEmulation(Properties params) {
		return getInt( SequenceStyleGenerator.INITIAL_PARAM, params, SequenceStyleGenerator.DEFAULT_INITIAL_VALUE );
	}

	protected int determineIncrementForTable(Properties params) {
		return getInt( TableGenerator.INCREMENT_PARAM, params, TableGenerator.DEFAULT_INITIAL_VALUE );
	}

	protected int determineIncrementForSequenceEmulation(Properties params) {
		return getInt( SequenceStyleGenerator.INCREMENT_PARAM, params, SequenceStyleGenerator.DEFAULT_INITIAL_VALUE );
	}

	protected String buildSelectQuery(Dialect dialect) {
		final String alias = "tbl";
		String query = "select " + StringHelper.qualify( alias, valueColumnName ) +
				" from " + renderedTableName + ' ' + alias;
		if (segmentColumnName != null) {
			query += " where " + StringHelper.qualify(alias, segmentColumnName) + "=?";
		}

		return dialect.applyLocksToSql(
				query,
				new LockOptions( LockMode.PESSIMISTIC_WRITE )
						.setAliasSpecificLockMode( alias, LockMode.PESSIMISTIC_WRITE ),
				Collections.singletonMap( alias, new String[] { valueColumnName } )
		);
	}

	protected String buildUpdateQuery(Dialect dialect) {
		String update = "update " + renderedTableName
				+ " set " + valueColumnName + "=?"
				+ " where " + valueColumnName + "=?";
		if (segmentColumnName != null) {
			update += " and " + segmentColumnName + "=?";
		}
		return update;
	}

	protected String buildInsertQuery(Dialect dialect) {
		String insert = "insert into " + renderedTableName;
		if (segmentColumnName != null) {
			insert += " (" + segmentColumnName + ", " + valueColumnName + ") " + " values (?, ?)";
		}
		else {
			insert += " (" + valueColumnName + ") " + " values (?)";
		}
		return insert;
	}

}
