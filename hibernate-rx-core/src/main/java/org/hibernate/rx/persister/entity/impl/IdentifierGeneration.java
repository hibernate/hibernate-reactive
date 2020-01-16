package org.hibernate.rx.persister.entity.impl;

import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.id.PersistentIdentifierGenerator;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.SimpleValue;

import java.util.Properties;

public class IdentifierGeneration {

    static Properties identifierGeneratorProperties(
            Dialect dialect,
            SessionFactoryImplementor sessionFactory,
            PersistentClass rootClass) {

        Properties params = new Properties();

        String defaultCatalog = sessionFactory.getSettings().getDefaultCatalogName();
        String defaultSchema = sessionFactory.getSettings().getDefaultSchemaName();

        //if the hibernate-mapping did not specify a schema/catalog, use the defaults
        //specified by properties - but note that if the schema/catalog were specified
        //in hibernate-mapping, or as params, they will already be initialized and
        //will override the values set here (they are in identifierGeneratorProperties)
        if ( defaultSchema!=null ) {
            params.setProperty(PersistentIdentifierGenerator.SCHEMA, defaultSchema);
        }
        if ( defaultCatalog!=null ) {
            params.setProperty(PersistentIdentifierGenerator.CATALOG, defaultCatalog);
        }

        //pass the entity-name, if not a collection-id
        if (rootClass!=null) {
            params.setProperty( IdentifierGenerator.ENTITY_NAME, rootClass.getEntityName() );
            params.setProperty( IdentifierGenerator.JPA_ENTITY_NAME, rootClass.getJpaEntityName() );
        }

        SimpleValue identifier = (SimpleValue) rootClass.getIdentifier();

        //init the table here instead of earlier, so that we can get a quoted table name
        //TODO: would it be better to simply pass the qualified table name, instead of
        //      splitting it up into schema/catalog/table names
        String tableName = identifier.getTable().getQuotedName(dialect);
        params.setProperty( PersistentIdentifierGenerator.TABLE, tableName );

        //pass the column name (a generated id almost always has a single column)
        Column column = (Column) identifier.getColumnIterator().next();
        String columnName = column.getQuotedName(dialect);
        params.setProperty( PersistentIdentifierGenerator.PK, columnName );

        params.putAll( identifier.getIdentifierGeneratorProperties() );

        return params;
    }

}
