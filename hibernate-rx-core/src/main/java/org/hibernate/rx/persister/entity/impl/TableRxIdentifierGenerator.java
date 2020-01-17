package org.hibernate.rx.persister.entity.impl;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.boot.model.relational.Database;
import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.boot.model.relational.QualifiedNameParser;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.config.spi.ConfigurationService;
import org.hibernate.engine.config.spi.StandardConverters;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.id.IdentifierGenerator;
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

import static org.hibernate.id.PersistentIdentifierGenerator.CATALOG;
import static org.hibernate.id.PersistentIdentifierGenerator.SCHEMA;
import static org.hibernate.id.enhanced.TableGenerator.*;

public class TableRxIdentifierGenerator implements RxIdentifierGenerator {

    private static final RxQueryExecutor queryExecutor = new RxQueryExecutor();

    private final String selectQuery;
    private final String updateQuery;
    private final String insertQuery;
    private final String segmentColumnName;
    private final String valueColumnName;
    private final String renderedTableName;
    private final boolean storeLastUsedValue;
    private final int initialValue;
    private final String segmentValue;

    @Override
    public CompletionStage<Optional<Integer>> generate(SessionFactoryImplementor factory) {
        return queryExecutor.selectInteger( selectQuery, new Object[] { segmentValue }, factory )
                .thenCompose( result -> {
                    if ( !result.isPresent() ) {
                        int initializationValue = storeLastUsedValue ? initialValue - 1 : initialValue;
                        return queryExecutor.update( insertQuery, new Object[] { segmentValue, initializationValue }, factory )
                                .thenApply( v -> Optional.of( initialValue ) );
                    }
                    else {
                        int currentValue = result.get();
                        int updatedValue = currentValue + 1;
                        return queryExecutor.update( updateQuery, new Object[] { updatedValue, currentValue, segmentValue }, factory )
                                .thenApply( v -> Optional.of( storeLastUsedValue ? updatedValue : currentValue ) );
                    }
                });
    }

    TableRxIdentifierGenerator(PersistentClass persistentClass, PersisterCreationContext creationContext) {

        MetadataImplementor metadata = creationContext.getMetadata();
        SessionFactoryImplementor sessionFactory = creationContext.getSessionFactory();
        Database database = metadata.getDatabase();
        JdbcEnvironment jdbcEnvironment = database.getJdbcEnvironment();

        Properties props = IdentifierGeneration.identifierGeneratorProperties(
                jdbcEnvironment.getDialect(),
                sessionFactory,
                persistentClass
        );

        storeLastUsedValue = database.getServiceRegistry().getService( ConfigurationService.class )
                .getSetting( AvailableSettings.TABLE_GENERATOR_STORE_LAST_USED, StandardConverters.BOOLEAN, true );

        String fallbackTableName = TableGenerator.DEF_TABLE;
        final Boolean preferGeneratorNameAsDefaultName = database.getServiceRegistry().getService( ConfigurationService.class )
                .getSetting( AvailableSettings.PREFER_GENERATOR_NAME_AS_DEFAULT_SEQUENCE_NAME, StandardConverters.BOOLEAN, true );
        if ( preferGeneratorNameAsDefaultName ) {
            final String generatorName = props.getProperty( IdentifierGenerator.GENERATOR_NAME );
            if ( StringHelper.isNotEmpty( generatorName ) ) {
                fallbackTableName = generatorName;
            }
        }

        String tableName = ConfigurationHelper.getString( TableGenerator.TABLE_PARAM, props, fallbackTableName );

        QualifiedNameParser.NameParts qualifiedTableName;
        if ( tableName.contains( "." ) ) {
            qualifiedTableName = QualifiedNameParser.INSTANCE.parse( tableName );
        }
        else {
            // todo : need to incorporate implicit catalog and schema names
            final Identifier catalog = jdbcEnvironment.getIdentifierHelper().toIdentifier(
                    ConfigurationHelper.getString( CATALOG, props )
            );
            final Identifier schema = jdbcEnvironment.getIdentifierHelper().toIdentifier(
                    ConfigurationHelper.getString( SCHEMA, props )
            );
            qualifiedTableName = new QualifiedNameParser.NameParts(
                    catalog,
                    schema,
                    jdbcEnvironment.getIdentifierHelper().toIdentifier( tableName )
            );
        }

        segmentColumnName = determineSegmentColumnName( props, jdbcEnvironment );
        valueColumnName = determineValueColumnName( props, jdbcEnvironment );

        segmentValue = determineSegmentValue( props );
        initialValue = determineInitialValue( props );

        final Namespace namespace = database.locateNamespace(
                qualifiedTableName.getCatalogName(),
                qualifiedTableName.getSchemaName()
        );

        Table table = namespace.locateTable( qualifiedTableName.getObjectName() );

        // allow physical naming strategies a chance to kick in
        final Dialect dialect = database.getJdbcEnvironment().getDialect();
        renderedTableName = database.getJdbcEnvironment().getQualifiedObjectNameFormatter().format(
                table.getQualifiedTableName(),
                dialect
        );

        this.selectQuery = buildSelectQuery( dialect );
        this.updateQuery = buildUpdateQuery();
        this.insertQuery = buildInsertQuery();
    }

    protected String determineSegmentColumnName(Properties params, JdbcEnvironment jdbcEnvironment) {
        final String name = ConfigurationHelper.getString( SEGMENT_COLUMN_PARAM, params, DEF_SEGMENT_COLUMN );
        return jdbcEnvironment.getIdentifierHelper().toIdentifier( name ).render( jdbcEnvironment.getDialect() );
    }

    protected String determineValueColumnName(Properties params, JdbcEnvironment jdbcEnvironment) {
        final String name = ConfigurationHelper.getString( VALUE_COLUMN_PARAM, params, DEF_VALUE_COLUMN );
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

    protected int determineInitialValue(Properties params) {
        return ConfigurationHelper.getInt( INITIAL_PARAM, params, DEFAULT_INITIAL_VALUE );
    }

    protected String buildSelectQuery(Dialect dialect) {
        final String alias = "tbl";
        final String query = "select " + StringHelper.qualify( alias, valueColumnName ) +
                " from " + renderedTableName + ' ' + alias +
                " where " + StringHelper.qualify( alias, segmentColumnName ) + "=$1";
        final LockOptions lockOptions = new LockOptions( LockMode.PESSIMISTIC_WRITE );
        lockOptions.setAliasSpecificLockMode( alias, LockMode.PESSIMISTIC_WRITE );
        final Map updateTargetColumnsMap = Collections.singletonMap( alias, new String[] { valueColumnName } );
        return dialect.applyLocksToSql( query, lockOptions, updateTargetColumnsMap );
    }

    protected String buildUpdateQuery() {
        return "update " + renderedTableName +
                " set " + valueColumnName + "=$1" +
                " where " + valueColumnName + "=$2 and " + segmentColumnName + "=$3";
    }

    protected String buildInsertQuery() {
        return "insert into " + renderedTableName + " (" + segmentColumnName + ", " + valueColumnName + ") " + " values ($1,$2)";
    }

}
