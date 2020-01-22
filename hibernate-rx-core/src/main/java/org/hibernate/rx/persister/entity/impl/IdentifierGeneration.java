package org.hibernate.rx.persister.entity.impl;

import org.hibernate.HibernateException;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.boot.model.relational.Database;
import org.hibernate.boot.model.relational.QualifiedName;
import org.hibernate.boot.model.relational.QualifiedNameParser;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.config.spi.ConfigurationService;
import org.hibernate.engine.config.spi.StandardConverters;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.id.*;
import org.hibernate.id.enhanced.*;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.SimpleValue;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.hibernate.rx.util.impl.RxUtil;

import java.util.Optional;
import java.util.Properties;

import static org.hibernate.id.enhanced.SequenceStyleGenerator.CATALOG;
import static org.hibernate.id.enhanced.SequenceStyleGenerator.SCHEMA;

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
		//	  splitting it up into schema/catalog/table names
		String tableName = identifier.getTable().getQuotedName(dialect);
		params.setProperty( PersistentIdentifierGenerator.TABLE, tableName );

		//pass the column name (a generated id almost always has a single column)
		Column column = (Column) identifier.getColumnIterator().next();
		String columnName = column.getQuotedName(dialect);
		params.setProperty( PersistentIdentifierGenerator.PK, columnName );

		params.putAll( identifier.getIdentifierGeneratorProperties() );

		return params;
	}

	static RxIdentifierGenerator<?> asRxGenerator(PersistentClass persistentClass, PersisterCreationContext creationContext, IdentifierGenerator identifierGenerator) {
		if (identifierGenerator instanceof SequenceStyleGenerator) {
			DatabaseStructure structure = ((SequenceStyleGenerator) identifierGenerator).getDatabaseStructure();
			if (structure instanceof TableStructure) {
				return new TableRxIdentifierGenerator(persistentClass, creationContext);
			}
			else if (structure instanceof SequenceStructure) {
				return new SequenceRxIdentifierGenerator(persistentClass, creationContext);
			}
			throw new IllegalStateException("unknown structure type");
		}
		else if (identifierGenerator instanceof TableGenerator) {
			return new TableRxIdentifierGenerator(persistentClass, creationContext);
		}
		else if (identifierGenerator instanceof SequenceGenerator) {
			return new SequenceRxIdentifierGenerator(persistentClass, creationContext);
		}
		else if (identifierGenerator instanceof SelectGenerator) {
			throw new HibernateException("SelectGenerator is not yet supported");
		}
		else if (identifierGenerator instanceof IdentityGenerator) {
			if (!creationContext.getSessionFactory().getSessionFactoryOptions()
					.isGetGeneratedKeysEnabled()
				&& !creationContext.getSessionFactory().getJdbcServices().getDialect()
					.getIdentityColumnSupport().supportsInsertSelectIdentity() ) {
				throw new HibernateException("getGeneratedKeys() is disabled");
			}
			return f -> RxUtil.completedFuture( Optional.empty() );
		}
		else if (identifierGenerator instanceof Assigned
				|| identifierGenerator instanceof CompositeNestedGeneratedValueGenerator) {
			//TODO!
			return f -> RxUtil.completedFuture( Optional.empty() );
		}
		else {
			return f -> RxUtil.completedFuture( Optional.of( identifierGenerator.generate(f, null) ) );
		}
	}

	/**
	 * Determine the name of the sequence (or table if this resolves to a physical table)
	 * to use.
	 *
	 * @param params The params supplied in the generator config (plus some standard useful extras).
	 * @param jdbcEnv The JdbcEnvironment
	 * @return The sequence name
	 */
	static QualifiedName determineSequenceName(
			Properties params,
//			Dialect dialect,
			JdbcEnvironment jdbcEnv,
			Database database) {
		final String sequencePerEntitySuffix = ConfigurationHelper.getString( SequenceStyleGenerator.CONFIG_SEQUENCE_PER_ENTITY_SUFFIX, params, SequenceStyleGenerator.DEF_SEQUENCE_SUFFIX );

		String fallbackSequenceName = SequenceStyleGenerator.DEF_SEQUENCE_NAME;
		final Boolean preferGeneratorNameAsDefaultName = database.getServiceRegistry().getService( ConfigurationService.class )
				.getSetting( AvailableSettings.PREFER_GENERATOR_NAME_AS_DEFAULT_SEQUENCE_NAME, StandardConverters.BOOLEAN, true );
		if ( preferGeneratorNameAsDefaultName ) {
			final String generatorName = params.getProperty( IdentifierGenerator.GENERATOR_NAME );
			if ( StringHelper.isNotEmpty( generatorName ) ) {
				fallbackSequenceName = generatorName;
			}
		}

		// JPA_ENTITY_NAME value honors <class ... entity-name="..."> (HBM) and @Entity#name (JPA) overrides.
		final String defaultSequenceName = ConfigurationHelper.getBoolean( SequenceStyleGenerator.CONFIG_PREFER_SEQUENCE_PER_ENTITY, params, false )
				? params.getProperty( SequenceStyleGenerator.JPA_ENTITY_NAME ) + sequencePerEntitySuffix
				: fallbackSequenceName;

		final String sequenceName = ConfigurationHelper.getString( SequenceStyleGenerator.SEQUENCE_PARAM, params, defaultSequenceName );
		if ( sequenceName.contains( "." ) ) {
			return QualifiedNameParser.INSTANCE.parse( sequenceName );
		}
		else {
			// todo : need to incorporate implicit catalog and schema names
			final Identifier catalog = jdbcEnv.getIdentifierHelper().toIdentifier(
					ConfigurationHelper.getString( CATALOG, params )
			);
			final Identifier schema =  jdbcEnv.getIdentifierHelper().toIdentifier(
					ConfigurationHelper.getString( SCHEMA, params )
			);
			return new QualifiedNameParser.NameParts(
					catalog,
					schema,
					jdbcEnv.getIdentifierHelper().toIdentifier( sequenceName )
			);
		}
	}

	static QualifiedName determineTableName(Database database, JdbcEnvironment jdbcEnvironment, Properties props) {
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
		return qualifiedTableName;
	}
}
