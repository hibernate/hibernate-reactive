package org.hibernate.rx.persister.entity.impl;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.boot.model.relational.Database;
import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.boot.model.relational.QualifiedName;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.config.spi.ConfigurationService;
import org.hibernate.engine.config.spi.StandardConverters;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.id.enhanced.SequenceStyleGenerator;
import org.hibernate.id.enhanced.TableGenerator;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Table;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.hibernate.rx.impl.RxQueryExecutor;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletionStage;

import static org.hibernate.id.enhanced.TableGenerator.*;

/**
 * Support for JPA's {@link javax.persistence.TableGenerator}. This
 * generator functions in two different modes: as a table generator
 * where different logical sequences are represented by different
 * rows ("segments"), or as an emulated sequence generator with
 * just one row and one column.
 */
public class TableRxIdentifierGenerator implements RxIdentifierGenerator {

	private static final RxQueryExecutor queryExecutor = new RxQueryExecutor();

	private final String selectQuery;
	private final String updateQuery;
	private final String insertQuery;
	private String segmentColumnName;
	private String valueColumnName;
	private final String renderedTableName;
	private final boolean storeLastUsedValue;
	private final int initialValue;
	private final String segmentValue;

	@Override
	public CompletionStage<Optional<Integer>> generate(SessionFactoryImplementor factory) {
		Object[] param = segmentColumnName == null ? new Object[] {} : new Object[] {segmentValue};
		return queryExecutor.selectInteger( selectQuery, param, factory )
				.thenCompose( result -> {
					if ( !result.isPresent() ) {
						int initializationValue = storeLastUsedValue ? initialValue - 1 : initialValue;
						Object[] params = segmentColumnName == null ?
								new Object[] {initializationValue} :
								new Object[] {segmentValue, initializationValue};
						return queryExecutor.update( insertQuery, params, factory )
								.thenApply( v -> Optional.of( initialValue ) );
					}
					else {
						int currentValue = result.get();
						int updatedValue = currentValue + 1;
						Object[] params = segmentColumnName == null ?
								new Object[] {updatedValue, currentValue} :
								new Object[] {updatedValue, currentValue, segmentValue};
						return queryExecutor.update( updateQuery, params, factory )
								.thenApply( v -> Optional.of( storeLastUsedValue ? updatedValue : currentValue ) );
					}
				});
	}

	TableRxIdentifierGenerator(PersistentClass persistentClass, PersisterCreationContext creationContext) {

		MetadataImplementor metadata = creationContext.getMetadata();
		SessionFactoryImplementor sessionFactory = creationContext.getSessionFactory();
		Database database = metadata.getDatabase();
		JdbcEnvironment jdbcEnvironment = database.getJdbcEnvironment();
		Dialect dialect = database.getJdbcEnvironment().getDialect();

		Properties props = IdentifierGeneration.identifierGeneratorProperties(
				jdbcEnvironment.getDialect(),
				sessionFactory,
				persistentClass
		);

		QualifiedName qualifiedTableName = IdentifierGeneration.determineTableName(database, jdbcEnvironment, props);
		final Namespace namespace = database.locateNamespace(
				qualifiedTableName.getCatalogName(),
				qualifiedTableName.getSchemaName()
		);
		Table table = namespace.locateTable( qualifiedTableName.getObjectName() );

		if (table == null) {
			//it's a SequenceStyleGenerator backed by a table
			qualifiedTableName = IdentifierGeneration.determineSequenceName(props, jdbcEnvironment, database);
			renderedTableName = qualifiedTableName.render();

			valueColumnName = determineValueColumnNameForSequenceEmulation(props, jdbcEnvironment);
			segmentColumnName = null;
			segmentValue = null;
			initialValue = determineInitialValueForSequenceEmulation( props );

		}
		else {
			//It's a regular TableGenerator
			segmentColumnName = determineSegmentColumnName( props, jdbcEnvironment );
			valueColumnName = determineValueColumnNameForTable( props, jdbcEnvironment );
			segmentValue = determineSegmentValue( props );
			initialValue = determineInitialValueForTable( props );

			// allow physical naming strategies a chance to kick in
			renderedTableName = database.getJdbcEnvironment().getQualifiedObjectNameFormatter().format(
					table.getQualifiedTableName(),
					dialect
			);
		}

		this.selectQuery = buildSelectQuery( dialect );
		this.updateQuery = buildUpdateQuery();
		this.insertQuery = buildInsertQuery();

		storeLastUsedValue = database.getServiceRegistry().getService( ConfigurationService.class )
				.getSetting( AvailableSettings.TABLE_GENERATOR_STORE_LAST_USED, StandardConverters.BOOLEAN, true );
	}

	protected String determineSegmentColumnName(Properties params, JdbcEnvironment jdbcEnvironment) {
		final String name = ConfigurationHelper.getString( SEGMENT_COLUMN_PARAM, params, DEF_SEGMENT_COLUMN );
		return jdbcEnvironment.getIdentifierHelper().toIdentifier( name ).render( jdbcEnvironment.getDialect() );
	}

	protected String determineValueColumnNameForTable(Properties params, JdbcEnvironment jdbcEnvironment) {
		final String name = ConfigurationHelper.getString(TableGenerator.VALUE_COLUMN_PARAM, params, TableGenerator.DEF_VALUE_COLUMN );
		return jdbcEnvironment.getIdentifierHelper().toIdentifier( name ).render( jdbcEnvironment.getDialect() );
	}

	static String determineValueColumnNameForSequenceEmulation(Properties params, JdbcEnvironment jdbcEnvironment) {
		final String name = ConfigurationHelper.getString( SequenceStyleGenerator.VALUE_COLUMN_PARAM, params, SequenceStyleGenerator.DEF_VALUE_COLUMN );
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
		final boolean preferSegmentPerEntity = ConfigurationHelper.getBoolean( CONFIG_PREFER_SEGMENT_PER_ENTITY, params, false );
		final String defaultToUse = preferSegmentPerEntity ? params.getProperty( TABLE ) : DEF_SEGMENT_VALUE;
		return defaultToUse;
	}

	protected int determineInitialValueForTable(Properties params) {
		return ConfigurationHelper.getInt( TableGenerator.INITIAL_PARAM, params, TableGenerator.DEFAULT_INITIAL_VALUE );
	}

	protected int determineInitialValueForSequenceEmulation(Properties params) {
		return ConfigurationHelper.getInt( SequenceStyleGenerator.INITIAL_PARAM, params, SequenceStyleGenerator.DEFAULT_INITIAL_VALUE );
	}
	protected String buildSelectQuery(Dialect dialect) {
		final String alias = "tbl";
		String query = "select " + StringHelper.qualify( alias, valueColumnName ) +
				" from " + renderedTableName + ' ' + alias;
		if (segmentColumnName != null) {
			query += " where " + StringHelper.qualify(alias, segmentColumnName) + "=$1";
		}
		final LockOptions lockOptions = new LockOptions( LockMode.PESSIMISTIC_WRITE );
		lockOptions.setAliasSpecificLockMode( alias, LockMode.PESSIMISTIC_WRITE );
		final Map updateTargetColumnsMap = Collections.singletonMap( alias, new String[] { valueColumnName } );
		return dialect.applyLocksToSql( query, lockOptions, updateTargetColumnsMap );
	}

	protected String buildUpdateQuery() {
		String update = "update " + renderedTableName +
				" set " + valueColumnName + "=$1" +
				" where " + valueColumnName + "=$2";
		if (segmentColumnName != null) {
			update += "and " + segmentColumnName + "=$3";
		}
		return update;
	}

	protected String buildInsertQuery() {
		String insert = "insert into " + renderedTableName;
		if (segmentColumnName != null) {
			insert += " (" + segmentColumnName + ", " + valueColumnName + ") " + " values (?,?)";
		}
		else {
			insert += " (" + valueColumnName + ") " + " values (?)";
		}
		return insert;
	}

}
