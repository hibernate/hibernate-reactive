/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.boot.model.relational.QualifiedName;
import org.hibernate.boot.model.relational.QualifiedNameParser;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.engine.config.spi.ConfigurationService;
import org.hibernate.engine.config.spi.StandardConverters;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.id.enhanced.SequenceStyleGenerator;
import org.hibernate.id.enhanced.TableGenerator;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.service.ServiceRegistry;

import java.util.Properties;

import static org.hibernate.id.enhanced.SequenceStyleGenerator.CATALOG;
import static org.hibernate.id.enhanced.SequenceStyleGenerator.SCHEMA;

public class IdentifierGeneration {

	/**
	 * Determine the name of the sequence (or table if this resolves to a physical table)
	 * to use.
	 *
	 * @param params The params supplied in the generator config (plus some standard useful extras).
	 * @return The sequence name
	 */
	static QualifiedName determineSequenceName(Properties params, ServiceRegistry serviceRegistry) {
		final String sequencePerEntitySuffix = ConfigurationHelper.getString( SequenceStyleGenerator.CONFIG_SEQUENCE_PER_ENTITY_SUFFIX, params, SequenceStyleGenerator.DEF_SEQUENCE_SUFFIX );

		String fallbackSequenceName = SequenceStyleGenerator.DEF_SEQUENCE_NAME;
		final Boolean preferGeneratorNameAsDefaultName = serviceRegistry.getService( ConfigurationService.class )
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
			JdbcEnvironment jdbcEnvironment = serviceRegistry.getService( JdbcEnvironment.class );
			// todo : need to incorporate implicit catalog and schema names
			final Identifier catalog = jdbcEnvironment.getIdentifierHelper().toIdentifier(
					ConfigurationHelper.getString( CATALOG, params )
			);
			final Identifier schema =  jdbcEnvironment.getIdentifierHelper().toIdentifier(
					ConfigurationHelper.getString( SCHEMA, params )
			);
			return new QualifiedNameParser.NameParts(
					catalog,
					schema,
					jdbcEnvironment.getIdentifierHelper().toIdentifier( sequenceName )
			);
		}
	}

	static QualifiedName determineTableName(Properties params, ServiceRegistry serviceRegistry) {
		String fallbackTableName = TableGenerator.DEF_TABLE;
		final Boolean preferGeneratorNameAsDefaultName = serviceRegistry.getService( ConfigurationService.class )
				.getSetting( AvailableSettings.PREFER_GENERATOR_NAME_AS_DEFAULT_SEQUENCE_NAME, StandardConverters.BOOLEAN, true );
		if ( preferGeneratorNameAsDefaultName ) {
			final String generatorName = params.getProperty( IdentifierGenerator.GENERATOR_NAME );
			if ( StringHelper.isNotEmpty( generatorName ) ) {
				fallbackTableName = generatorName;
			}
		}

		String tableName = ConfigurationHelper.getString( TableGenerator.TABLE_PARAM, params, fallbackTableName );

		QualifiedNameParser.NameParts qualifiedTableName;
		if ( tableName.contains( "." ) ) {
			qualifiedTableName = QualifiedNameParser.INSTANCE.parse( tableName );
		}
		else {
			JdbcEnvironment jdbcEnvironment = serviceRegistry.getService( JdbcEnvironment.class );
			// todo : need to incorporate implicit catalog and schema names
			final Identifier catalog = jdbcEnvironment.getIdentifierHelper().toIdentifier(
					ConfigurationHelper.getString( CATALOG, params )
			);
			final Identifier schema = jdbcEnvironment.getIdentifierHelper().toIdentifier(
					ConfigurationHelper.getString( SCHEMA, params )
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
