/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import org.hibernate.HibernateException;
import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.dialect.internal.DialectFactoryImpl;
import org.hibernate.engine.jdbc.dialect.spi.DialectFactory;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfoSource;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.testing.SqlStatementTracker;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.cfg.JdbcSettings.ALLOW_METADATA_ON_BOOT;
import static org.hibernate.cfg.JdbcSettings.DIALECT;
import static org.hibernate.cfg.JdbcSettings.JAKARTA_HBM2DDL_DB_NAME;
import static org.hibernate.cfg.JdbcSettings.JAKARTA_JDBC_URL;
import static org.hibernate.reactive.BaseReactiveTest.setSqlLoggingProperties;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Hibernate ORM allows starting up without access to the DB ("offline")
 * when {@link Settings#ALLOW_METADATA_ON_BOOT} is set to false.
 * <p>
 * Inspired by the test
 * {@code org.hibernate.orm.test.boot.database.metadata.MetadataAccessTests}
 * in Hibernate ORM.
 * </p>
 */
public class MetadataAccessTest {

	private static SqlStatementTracker sqlTracker;

	private static final int EXPECTED_MAJOR = 123;
	private static final int EXPECTED_MINOR = 456;
	private static final DatabaseVersion EXPECTED_VERSION = DatabaseVersion.make( EXPECTED_MAJOR, EXPECTED_MINOR );

	private static Properties dialectMajorMinorProperties() {
		Properties dbProperties = new Properties();
		// Major and Minor should override the full version, so we keep them different
		dbProperties.setProperty( Settings.DIALECT_DB_MAJOR_VERSION, String.valueOf( EXPECTED_MAJOR ) );
		dbProperties.setProperty( Settings.DIALECT_DB_MINOR_VERSION, String.valueOf( EXPECTED_MINOR ) );
		return dbProperties;
	}

	private static Properties jakartaMajorMinorProperties() {
		Properties dbProperties = new Properties();
		dbProperties.setProperty( Settings.JAKARTA_HBM2DDL_DB_MAJOR_VERSION, String.valueOf( EXPECTED_MAJOR ) );
		dbProperties.setProperty( Settings.JAKARTA_HBM2DDL_DB_MINOR_VERSION, String.valueOf( EXPECTED_MINOR ) );
		return dbProperties;
	}

	private static Properties jakartaFullDbVersion() {
		Properties dbProperties = new Properties();
		dbProperties.setProperty( Settings.JAKARTA_HBM2DDL_DB_VERSION, EXPECTED_MAJOR + "." + EXPECTED_MINOR );
		return dbProperties;
	}

	private static Properties dialectFullDbVersion() {
		Properties dbProperties = new Properties();
		dbProperties.setProperty( Settings.DIALECT_DB_VERSION, EXPECTED_MAJOR + "." + EXPECTED_MINOR );
		return dbProperties;
	}

	static Stream<Arguments> explicitVersionProperties() {
		return Stream.of(
				arguments( "Jakarta properties", jakartaMajorMinorProperties() ),
				arguments( "Deprecated dialect properties", dialectMajorMinorProperties() ),
				arguments( "Jakarta db version property", jakartaFullDbVersion() ),
				arguments( "Deprecated dialect db version property", dialectFullDbVersion() )
		);
	}

	@ParameterizedTest(name = "Test {0} with " + DIALECT)
	@MethodSource("explicitVersionProperties")
	public void testExplicitVersionWithDialect(String display, Properties dbProperties) {
		dbProperties.setProperty( ALLOW_METADATA_ON_BOOT, "false" );
		dbProperties.setProperty( DIALECT, dbType().getDialectClass().getName() );

		try (StandardServiceRegistry serviceRegistry = createServiceRegistry( dbProperties )) {
			final Dialect dialect = dialect( serviceRegistry );
			assertThat( dialect ).isInstanceOf( dbType().getDialectClass() );
			assertThat( dialect.getVersion() ).isEqualTo( EXPECTED_VERSION );
		}

		assertThat( sqlTracker.getLoggedQueries() )
				.as( "No query should be executed at start up" )
				.isEmpty();
	}

	@ParameterizedTest(name = "Test {0} with " + JAKARTA_HBM2DDL_DB_NAME)
	@MethodSource("explicitVersionProperties")
	public void testExplicitVersionWithJakartaDbName(String display, Properties dbProperties) {
		dbProperties.setProperty( ALLOW_METADATA_ON_BOOT, "false" );
		dbProperties.setProperty( JAKARTA_HBM2DDL_DB_NAME, dbType().getProductName() );

		try (StandardServiceRegistry serviceRegistry = createServiceRegistry( dbProperties )) {
			final Dialect dialect = dialect( serviceRegistry );
			assertThat( dialect ).isInstanceOf( dbType().getDialectClass() );
			assertThat( dialect.getVersion() ).isEqualTo( EXPECTED_VERSION );
		}

		assertThat( sqlTracker.getLoggedQueries() )
				.as( "No query should be executed at start up" )
				.isEmpty();
	}

	@Test
	public void testMinimumDatabaseVersionWithDialect() {
		final Properties dbProperties = new Properties();
		dbProperties.setProperty( ALLOW_METADATA_ON_BOOT, "false" );
		dbProperties.setProperty( DIALECT, dbType().getDialectClass().getName() );

		try (StandardServiceRegistry serviceRegistry = createServiceRegistry( dbProperties )) {
			final Dialect dialect = dialect( serviceRegistry );
			assertThat( dialect ).isInstanceOf( dbType().getDialectClass() );
			assertThat( dialect.getVersion() ).isEqualTo( dbType().getMinimumVersion() );
		}

		assertThat( sqlTracker.getLoggedQueries() )
				.as( "No query should be executed at start up" )
				.isEmpty();
	}

	@Test
	public void testMinimumDatabaseVersionWithJakartaDbName() {
		final Properties dbProperties = new Properties();
		dbProperties.setProperty( ALLOW_METADATA_ON_BOOT, "false" );
		dbProperties.setProperty( JAKARTA_HBM2DDL_DB_NAME, dbType().getProductName() );

		try (StandardServiceRegistry serviceRegistry = createServiceRegistry( dbProperties )) {
			final Dialect dialect = dialect( serviceRegistry );
			assertThat( dialect ).isInstanceOf( dbType().getDialectClass() );
			assertThat( dialect.getVersion() ).isEqualTo( dbType().getMinimumVersion() );
		}

		assertThat( sqlTracker.getLoggedQueries() )
				.as( "No query should be executed at start up" )
				.isEmpty();
	}

	@Test
	public void testDeterminedVersion() {
		final Properties disabledProperties = new Properties();
		disabledProperties.setProperty( ALLOW_METADATA_ON_BOOT, "false" );
		disabledProperties.setProperty( DIALECT, dbType().getDialectClass().getName() );
		// The dialect when ALLOW_METADATA_ON_BOOT si set to false
		final Dialect metadataDisabledDialect;
		try (StandardServiceRegistry serviceRegistry = createServiceRegistry( disabledProperties )) {
			metadataDisabledDialect = dialect( serviceRegistry );
			// We didn't set the version anywhere else, so we expect it to be the minimum version
			assertThat( metadataDisabledDialect.getVersion() ).isEqualTo( dbType().getMinimumVersion() );
		}

		assertThat( sqlTracker.getLoggedQueries() )
				.as( "No query should be executed at start up" )
				.isEmpty();

		final Properties enabledProperties = new Properties();
		enabledProperties.setProperty( ALLOW_METADATA_ON_BOOT, "true" );
		enabledProperties.setProperty( JAKARTA_JDBC_URL, DatabaseConfiguration.getJdbcUrl() );
		try (StandardServiceRegistry serviceRegistry = createServiceRegistry( enabledProperties )) {
			final Dialect metadataEnabledDialect = dialect( serviceRegistry );

			// We expect determineDatabaseVersion(), when called on metadataAccessDisabledDialect,
			// to return the version that would have been returned,
			// had we booted up with auto-detection of version (metadata access allowed).
			DatabaseVersion determinedDatabaseVersion = metadataDisabledDialect
					.determineDatabaseVersion( dialectResolutionInfo( serviceRegistry ) );

			// Whatever the version, we don't expect the minimum one
			assertThat( determinedDatabaseVersion ).isNotEqualTo( dbType().getMinimumVersion() );

			assertThat( determinedDatabaseVersion ).isEqualTo( metadataEnabledDialect.getVersion() );
			assertThat( determinedDatabaseVersion.getMajor() ).isEqualTo( metadataEnabledDialect.getVersion().getMajor() );
			assertThat( determinedDatabaseVersion.getMinor() ).isEqualTo( metadataEnabledDialect.getVersion().getMinor() );
			assertThat( determinedDatabaseVersion.getMicro() ).isEqualTo( metadataEnabledDialect.getVersion().getMicro() );
		}
	}

	private Configuration constructConfiguration(Properties properties) {
		Configuration configuration = new Configuration();
		setSqlLoggingProperties( configuration );
		configuration.addProperties( properties );

		// Construct a tracker that collects query statements via the SqlStatementLogger framework.
		// Pass in configuration properties to hand off any actual logging properties
		sqlTracker = new SqlStatementTracker( s -> true, configuration.getProperties() );
		return configuration;
	}

	private StandardServiceRegistry createServiceRegistry(Properties properties) {
		Configuration configuration = constructConfiguration( properties );
		StandardServiceRegistryBuilder builder = new ReactiveServiceRegistryBuilder();
		// We will set these properties when needed
		assertThat( builder.getSettings() ).doesNotContainKeys( DIALECT, JAKARTA_HBM2DDL_DB_NAME );
		builder.applySettings( configuration.getProperties() );

		builder.addInitiator( new CapturingDialectFactory.Initiator() );
		sqlTracker.registerService( builder );
		return builder.enableAutoClose().build();
	}

	private static Dialect dialect(StandardServiceRegistry registry) {
		return registry.getService( JdbcEnvironment.class ).getDialect();
	}

	private static DialectResolutionInfo dialectResolutionInfo(StandardServiceRegistry registry) {
		return ( (CapturingDialectFactory) registry.getService( DialectFactory.class ) )
				.capturedDialectResolutionInfoSource.getDialectResolutionInfo();
	}

	// A hack to easily retrieve DialectResolutionInfo exactly as it would be constructed by Hibernate ORM
	private static class CapturingDialectFactory extends DialectFactoryImpl {

		static class Initiator implements StandardServiceInitiator<DialectFactory> {
			@Override
			public Class<DialectFactory> getServiceInitiated() {
				return DialectFactory.class;
			}

			@Override
			public DialectFactory initiateService(Map<String, Object> configurationValues, ServiceRegistryImplementor registry) {
				return new CapturingDialectFactory();
			}
		}

		DialectResolutionInfoSource capturedDialectResolutionInfoSource;

		@Override
		public Dialect buildDialect(Map<String, Object> configValues, DialectResolutionInfoSource resolutionInfoSource)
				throws HibernateException {
			this.capturedDialectResolutionInfoSource = resolutionInfoSource;
			return super.buildDialect( configValues, resolutionInfoSource );
		}
	}
}
