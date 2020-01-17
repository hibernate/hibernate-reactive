package org.hibernate.rx.persister.entity.impl;

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
import org.hibernate.internal.util.StringHelper;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.hibernate.rx.impl.RxQueryExecutor;
import org.hibernate.rx.util.impl.RxUtil;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletionStage;

import static org.hibernate.id.enhanced.SequenceStyleGenerator.*;

public class SequenceRxIdentifierGenerator implements RxIdentifierGenerator {

    private static final RxQueryExecutor queryExecutor = new RxQueryExecutor();

    private final String sql;

    SequenceRxIdentifierGenerator(PersistentClass persistentClass, PersisterCreationContext creationContext) {

        MetadataImplementor metadata = creationContext.getMetadata();
        SessionFactoryImplementor sessionFactory = creationContext.getSessionFactory();
        Database database = metadata.getDatabase();
        JdbcEnvironment jdbcEnvironment = database.getJdbcEnvironment();

        Properties props = IdentifierGeneration.identifierGeneratorProperties(
                jdbcEnvironment.getDialect(),
                sessionFactory,
                persistentClass
        );

        final String sequencePerEntitySuffix = ConfigurationHelper.getString( CONFIG_SEQUENCE_PER_ENTITY_SUFFIX, props, DEF_SEQUENCE_SUFFIX );

        String fallbackSequenceName = DEF_SEQUENCE_NAME;
        final Boolean preferGeneratorNameAsDefaultName = database.getServiceRegistry().getService( ConfigurationService.class )
                .getSetting( AvailableSettings.PREFER_GENERATOR_NAME_AS_DEFAULT_SEQUENCE_NAME, StandardConverters.BOOLEAN, true );
        if ( preferGeneratorNameAsDefaultName ) {
            final String generatorName = props.getProperty( IdentifierGenerator.GENERATOR_NAME );
            if ( StringHelper.isNotEmpty( generatorName ) ) {
                fallbackSequenceName = generatorName;
            }
        }

        // JPA_ENTITY_NAME value honors <class ... entity-name="..."> (HBM) and @Entity#name (JPA) overrides.
        final String defaultSequenceName = ConfigurationHelper.getBoolean( CONFIG_PREFER_SEQUENCE_PER_ENTITY, props, false )
                ? props.getProperty( JPA_ENTITY_NAME ) + sequencePerEntitySuffix
                : fallbackSequenceName;

        QualifiedNameParser.NameParts logicalQualifiedSequenceName;
        final String sequenceName = ConfigurationHelper.getString( SEQUENCE_PARAM, props, defaultSequenceName );
        if ( sequenceName.contains( "." ) ) {
            logicalQualifiedSequenceName = QualifiedNameParser.INSTANCE.parse( sequenceName );
        }
        else {
            // todo : need to incorporate implicit catalog and schema names
            final Identifier catalog = jdbcEnvironment.getIdentifierHelper().toIdentifier(
                    ConfigurationHelper.getString( CATALOG, props)
            );
            final Identifier schema =  jdbcEnvironment.getIdentifierHelper().toIdentifier(
                    ConfigurationHelper.getString( SCHEMA, props)
            );
            logicalQualifiedSequenceName = new QualifiedNameParser.NameParts(
                    catalog,
                    schema,
                    jdbcEnvironment.getIdentifierHelper().toIdentifier( sequenceName )
            );
        }
        final Namespace namespace = database.locateNamespace(
                logicalQualifiedSequenceName.getCatalogName(),
                logicalQualifiedSequenceName.getSchemaName()
        );
        org.hibernate.boot.model.relational.Sequence sequence = namespace.locateSequence( logicalQualifiedSequenceName.getObjectName() );

        if (sequence != null) {
            final Dialect dialect = jdbcEnvironment.getDialect();
            String finalSequenceName = jdbcEnvironment.getQualifiedObjectNameFormatter().format(
                    sequence.getName(),
                    dialect
            );
            sql = dialect.getSequenceNextValString(finalSequenceName);
        }
        else {
            sql = null;
        }
    }

    public CompletionStage<Optional<Integer>> generate(SessionFactoryImplementor factory) {
        return sql==null ? RxUtil.completedFuture(Optional.empty())
                : queryExecutor.selectInteger(sql, new Object[0], factory);
    }
}
