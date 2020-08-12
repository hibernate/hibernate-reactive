/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import org.hibernate.HibernateException;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.boot.model.relational.QualifiedName;
import org.hibernate.boot.model.relational.QualifiedNameParser;
import org.hibernate.engine.config.spi.ConfigurationService;
import org.hibernate.engine.config.spi.StandardConverters;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.IdentifierGenerationException;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.id.enhanced.SequenceStyleGenerator;
import org.hibernate.id.enhanced.TableGenerator;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.IntegerType;
import org.hibernate.type.LongType;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.CompletionStage;

import static org.hibernate.id.enhanced.SequenceStyleGenerator.CATALOG;
import static org.hibernate.id.enhanced.SequenceStyleGenerator.SCHEMA;
import static org.hibernate.internal.util.config.ConfigurationHelper.getBoolean;
import static org.hibernate.internal.util.config.ConfigurationHelper.getString;

public class IdentifierGeneration {

	/**
	 * Determine the name of the sequence (or table if this resolves to a physical table)
	 * to use.
	 *
	 * @param params The params supplied in the generator config (plus some standard useful extras).
	 * @return The sequence name
	 */
	static QualifiedName determineSequenceName(Properties params, ServiceRegistry serviceRegistry) {
		final String sequencePerEntitySuffix =
				getString( SequenceStyleGenerator.CONFIG_SEQUENCE_PER_ENTITY_SUFFIX, params,
						SequenceStyleGenerator.DEF_SEQUENCE_SUFFIX );

		String fallbackSequenceName = SequenceStyleGenerator.DEF_SEQUENCE_NAME;
		if ( preferGeneratorNameAsDefaultName(serviceRegistry) ) {
			final String generatorName = params.getProperty( IdentifierGenerator.GENERATOR_NAME );
			if ( StringHelper.isNotEmpty( generatorName ) ) {
				fallbackSequenceName = generatorName;
			}
		}

		// JPA_ENTITY_NAME value honors <class ... entity-name="..."> (HBM) and @Entity#name (JPA) overrides.
		final String defaultSequenceName =
				getBoolean( SequenceStyleGenerator.CONFIG_PREFER_SEQUENCE_PER_ENTITY, params, false )
						? params.getProperty( SequenceStyleGenerator.JPA_ENTITY_NAME ) + sequencePerEntitySuffix
						: fallbackSequenceName;

		final String sequenceName = getString( SequenceStyleGenerator.SEQUENCE_PARAM, params, defaultSequenceName );
		if ( sequenceName.contains( "." ) ) {
			return QualifiedNameParser.INSTANCE.parse( sequenceName );
		}
		else {
			JdbcEnvironment jdbcEnvironment = serviceRegistry.getService( JdbcEnvironment.class );
			// todo : need to incorporate implicit catalog and schema names
			final Identifier catalog = jdbcEnvironment.getIdentifierHelper().toIdentifier(
					getString( CATALOG, params )
			);
			final Identifier schema =  jdbcEnvironment.getIdentifierHelper().toIdentifier(
					getString( SCHEMA, params )
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
		if ( preferGeneratorNameAsDefaultName(serviceRegistry) ) {
			final String generatorName = params.getProperty( IdentifierGenerator.GENERATOR_NAME );
			if ( StringHelper.isNotEmpty( generatorName ) ) {
				fallbackTableName = generatorName;
			}
		}

		String tableName = getString( TableGenerator.TABLE_PARAM, params, fallbackTableName );

		QualifiedNameParser.NameParts qualifiedTableName;
		if ( tableName.contains( "." ) ) {
			qualifiedTableName = QualifiedNameParser.INSTANCE.parse( tableName );
		}
		else {
			JdbcEnvironment jdbcEnvironment = serviceRegistry.getService( JdbcEnvironment.class );
			// todo : need to incorporate implicit catalog and schema names
			final Identifier catalog = jdbcEnvironment.getIdentifierHelper().toIdentifier(
					getString( CATALOG, params )
			);
			final Identifier schema = jdbcEnvironment.getIdentifierHelper().toIdentifier(
					getString( SCHEMA, params )
			);
			qualifiedTableName = new QualifiedNameParser.NameParts(
					catalog,
					schema,
					jdbcEnvironment.getIdentifierHelper().toIdentifier( tableName )
			);
		}
		return qualifiedTableName;
	}

	private static Boolean preferGeneratorNameAsDefaultName(ServiceRegistry serviceRegistry) {
		return serviceRegistry.getService(ConfigurationService.class)
				.getSetting( Settings.PREFER_GENERATOR_NAME_AS_DEFAULT_SEQUENCE_NAME,
						StandardConverters.BOOLEAN, true );
	}

	@SuppressWarnings("unchecked")
	public static CompletionStage<Serializable> generateId(Object entity, EntityPersister persister,
														   ReactiveConnectionSupplier connectionSupplier,
														   SharedSessionContractImplementor session) {
		IdentifierGenerator generator = persister.getIdentifierGenerator();
		return generator instanceof ReactiveIdentifierGenerator
				? ( (ReactiveIdentifierGenerator<Serializable>) generator ).generate( connectionSupplier, entity )
				: CompletionStages.completedFuture( generator.generate( session, entity ) );
	}

	public static Serializable assignIdIfNecessary(Object generatedId, Object entity,
													EntityPersister persister,
													SharedSessionContractImplementor session) {
		if ( generatedId != null ) {
			if ( generatedId instanceof Long ) {
				Long longId = (Long) generatedId;
				Type identifierType = persister.getIdentifierType();
				if ( identifierType == LongType.INSTANCE ) {
					return longId;
				}
				else if ( identifierType == IntegerType.INSTANCE ) {
					return longId.intValue();
				}
				else {
					throw new HibernateException(
							"cannot generate identifiers of type "
									+ identifierType.getReturnedClass().getSimpleName()
									+ " for: "
									+ persister.getEntityName()
					);
				}
			}
			else {
				return (Serializable) generatedId;
			}
		}
		else {
			Serializable assignedId = persister.getIdentifier( entity, session );
			if ( assignedId == null ) {
				throw new IdentifierGenerationException(
						"ids for this class must be manually assigned before calling save(): "
								+ persister.getEntityName()
				);
			}
			return assignedId;
		}
	}
}
