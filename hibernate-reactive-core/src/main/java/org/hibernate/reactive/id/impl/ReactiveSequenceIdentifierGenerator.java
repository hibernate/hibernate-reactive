/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import java.util.Properties;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.boot.model.relational.QualifiedName;
import org.hibernate.boot.model.relational.QualifiedNameParser;
import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.boot.registry.selector.spi.StrategySelector;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.config.spi.ConfigurationService;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.id.enhanced.ImplicitDatabaseObjectNamingStrategy;
import org.hibernate.id.enhanced.SequenceStyleGenerator;
import org.hibernate.id.enhanced.StandardNamingStrategy;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.Type;

import static org.hibernate.cfg.AvailableSettings.ID_DB_STRUCTURE_NAMING_STRATEGY;
import static org.hibernate.id.PersistentIdentifierGenerator.CATALOG;
import static org.hibernate.id.PersistentIdentifierGenerator.SCHEMA;
import static org.hibernate.internal.log.IncubationLogger.INCUBATION_LOGGER;
import static org.hibernate.internal.util.NullnessHelper.coalesceSuppliedValues;
import static org.hibernate.internal.util.config.ConfigurationHelper.getInt;

/**
 * Support for JPA's {@link jakarta.persistence.SequenceGenerator}.
 * <p>
 * This implementation supports block allocation, but does not
 * guarantee that generated identifiers are sequential.
 */
// FIXME: This class should extends SequenceStyleGenerator (probably)
//		  Now, I've just adapted the existing one we have in Hibernate Reactive 5
public class ReactiveSequenceIdentifierGenerator extends BlockingIdentifierGenerator implements IdentifierGenerator {

	public static final Object[] NO_PARAMS = new Object[0];
	private Dialect dialect;
	private QualifiedName qualifiedName;

	private String sql;
	private int increment;
	@Override
	protected int getBlockSize() {
		return increment;
	}

	@Override
	protected CompletionStage<Long> nextHiValue(ReactiveConnectionSupplier session) {
		return session.getReactiveConnection()
				.selectIdentifier( sql, NO_PARAMS, Long.class );
	}

	// First one to get called during initialization
	@Override
	public void configure(Type type, Properties properties, ServiceRegistry serviceRegistry) {
		JdbcEnvironment jdbcEnvironment = serviceRegistry.getService( JdbcEnvironment.class );
		dialect = jdbcEnvironment.getDialect();
		qualifiedName = determineSequenceName( properties, dialect, jdbcEnvironment, serviceRegistry );
		increment = determineIncrementForSequenceEmulation( properties );
	}

	/**
	 * Determine the name of the sequence (or table if this resolves to a physical table)
	 * to use.
	 * <p>
	 * Called during {@linkplain #configure configuration}.
	 *
	 * @param params The params supplied in the generator config (plus some standard useful extras).
	 * @param dialect The dialect in effect
	 * @param jdbcEnv The JdbcEnvironment
	 * @return The sequence name
	 * @see SequenceStyleGenerator#determineSequenceName(Properties, Dialect, JdbcEnvironment, ServiceRegistry)
	 */
	protected QualifiedName determineSequenceName(Properties params, Dialect dialect, JdbcEnvironment jdbcEnv, ServiceRegistry serviceRegistry) {
		final Identifier catalog = jdbcEnv.getIdentifierHelper()
				.toIdentifier( ConfigurationHelper.getString( CATALOG, params ) );
		final Identifier schema =  jdbcEnv.getIdentifierHelper()
				.toIdentifier( ConfigurationHelper.getString( SCHEMA, params ) );

		final String sequenceName = ConfigurationHelper.getString(
				SequenceStyleGenerator.SEQUENCE_PARAM,
				params,
				() -> ConfigurationHelper.getString( SequenceStyleGenerator.ALT_SEQUENCE_PARAM, params )
		);

		if ( StringHelper.isNotEmpty( sequenceName ) ) {
			// we have an explicit name, use it
			if ( sequenceName.contains( "." ) ) {
				return QualifiedNameParser.INSTANCE.parse( sequenceName );
			}
			else {
				return new QualifiedNameParser.NameParts(
						catalog,
						schema,
						jdbcEnv.getIdentifierHelper().toIdentifier( sequenceName )
				);
			}
		}

		// otherwise, determine an implicit name to use
		return determineImplicitName( catalog, schema, params, serviceRegistry );
	}

	/**
	 * A copy of the method with the same signature in Hibernate ORM {@link SequenceStyleGenerator}
	 */
	private QualifiedName determineImplicitName(Identifier catalog, Identifier schema, Properties params, ServiceRegistry serviceRegistry) {
		final StrategySelector strategySelector = serviceRegistry.getService( StrategySelector.class );

		final String namingStrategySetting = coalesceSuppliedValues(
				() -> {
					final String localSetting = ConfigurationHelper.getString( ID_DB_STRUCTURE_NAMING_STRATEGY, params );
					if ( localSetting != null ) {
						INCUBATION_LOGGER.incubatingSetting( ID_DB_STRUCTURE_NAMING_STRATEGY );
					}
					return localSetting;
				},
				() -> {
					final ConfigurationService configurationService = serviceRegistry.getService( ConfigurationService.class );
					final String globalSetting = ConfigurationHelper.getString( ID_DB_STRUCTURE_NAMING_STRATEGY, configurationService.getSettings() );
					if ( globalSetting != null ) {
						INCUBATION_LOGGER.incubatingSetting( ID_DB_STRUCTURE_NAMING_STRATEGY );
					}
					return globalSetting;
				},
				StandardNamingStrategy.class::getName
		);

		return strategySelector
				.resolveStrategy( ImplicitDatabaseObjectNamingStrategy.class, namingStrategySetting )
				.determineSequenceName( catalog, schema, params, serviceRegistry );
	}

	// Called after configure
	@Override
	public void initialize(SqlStringGenerationContext context) {
		String renderedSequenceName = context.format( qualifiedName );
		sql = dialect.getSequenceSupport().getSequenceNextValString( renderedSequenceName );
	}

	@Override
	public Object generate(SharedSessionContractImplementor session, Object object) throws HibernateException {
		// TODO: Do we need to implement this?
		throw new UnsupportedOperationException("Not yet implemented");
	}

	public QualifiedName getSequenceName() {
		return qualifiedName;
	}

	protected int determineIncrementForSequenceEmulation(Properties params) {
		return getInt( SequenceStyleGenerator.INCREMENT_PARAM, params, SequenceStyleGenerator.DEFAULT_INCREMENT_SIZE );
	}
}
