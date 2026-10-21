/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.schema;


import java.net.URL;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.annotations.DisabledFor;
import org.hibernate.reactive.annotations.EnabledFor;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.tool.schema.spi.SchemaManagementException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.cfg.SchemaToolingSettings.HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertThrown;
import static org.hibernate.tool.schema.JdbcMetadataAccessStrategy.GROUPED;
import static org.hibernate.tool.schema.JdbcMetadataAccessStrategy.INDIVIDUALLY;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Test schema validation at startup for all the supported types:
 * - Missing table validation error
 * - No validation error when everything is fine
 * - TODO: Test that validation fails when a column is missing
 * - TODO: Test that validation fails when a column is the wrong type
 */
@DisabledFor(value = DB2, reason = "We don't have an information extractor. See https://github.com/hibernate/hibernate-reactive/issues/911")
public class SchemaValidationTest extends BaseReactiveTest {

	static Stream<Arguments> settings() {
		return Stream.of(
				arguments( INDIVIDUALLY.toString(), null ),
				arguments( GROUPED.toString(), null ),
				arguments( INDIVIDUALLY.toString(), "VARBINARY" ),
				arguments( GROUPED.toString(), "VARBINARY" )
		);
	}

	protected Configuration constructConfiguration(String action, String strategy, String type) {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( Settings.HBM2DDL_AUTO, action );
		configuration.setProperty( HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY, strategy );
		if ( type != null ) {
			// ORM 7 stores arrays as json for MariaDB and MySQL. By setting this property, users
			// can keep the behaviour backward compatible. We need to test both.
			configuration.setProperty( "hibernate.type.preferred_array_jdbc_type", type );
		}
		return configuration;
	}

	@Override
	public void before(VertxTestContext context) {
		// Do nothing, we prepare everything in the test so that we can use a parameterized test
	}

	public CompletionStage<Void> setupFactory(String strategy, String type) {
		return setupFactory( strategy, type, null );
	}

	public CompletionStage<Void> setupFactory(String strategy, String type, String importFile) {
		Configuration createConf = constructConfiguration( "create", strategy, type );
		createConf.addAnnotatedClass( BasicTypesTestEntity.class );
		if ( importFile != null ) {
			final URL importFileURL = Thread.currentThread()
					.getContextClassLoader()
					.getResource( importFile );
			createConf.setProperty( AvailableSettings.JAKARTA_HBM2DDL_LOAD_SCRIPT_SOURCE, importFileURL.getFile() );
		}
		// Make sure that the extra table is not in the db
		Configuration dropConf = constructConfiguration( "drop", strategy, type );
		dropConf.addAnnotatedClass( Extra.class );

		return setupSessionFactory( dropConf )
				.thenCompose( v -> factoryManager.stop() )
				.thenCompose( v -> setupSessionFactory( createConf ) )
				.thenCompose( v -> factoryManager.stop() );
	}

	@AfterEach
	@Override
	public void after(VertxTestContext context) {
		super.after( context );
		closeFactory( context );
	}

	@ParameterizedTest
	@MethodSource("settings")
	@EnabledFor( ORACLE )
	public void testOracleColumnTypeValidation(final String strategy, final String type, VertxTestContext context) {
		test(
				context, setupFactory( strategy, type, "oracle-SchemaValidationTest.sql" )
						.thenCompose( v -> {
							Configuration validateConf = constructConfiguration( "validate", strategy, type );
							validateConf.addAnnotatedClass( Fruit.class );
							new StandardServiceRegistryBuilder().applySettings( validateConf.getProperties() );
							return setupSessionFactory( validateConf );
						} )
		);
	}

	// When we have created the table, the validation should pass
	@ParameterizedTest
	@MethodSource("settings")
	@Timeout(value = 10, timeUnit = MINUTES)
	public void testValidationSucceeds(final String strategy, final String type, VertxTestContext context) {
		test(
				context, setupFactory( strategy, type )
						.thenCompose( v -> {
							Configuration validateConf = constructConfiguration( "validate", strategy, type );
							validateConf.addAnnotatedClass( BasicTypesTestEntity.class );
							new StandardServiceRegistryBuilder().applySettings( validateConf.getProperties() );
							return setupSessionFactory( validateConf );
						} )
		);
	}


	// Validation should fail if a table is missing
	@ParameterizedTest
	@MethodSource("settings")
	@Timeout(value = 10, timeUnit = MINUTES)
	public void testValidationFails(String strategy, String type, VertxTestContext context) {
		final String errorMessage = "Schema validation: missing table [" + Extra.TABLE_NAME + "]";
		test(
				context, setupFactory( strategy, type )
						.thenCompose( v -> {
							Configuration validateConf = constructConfiguration( "validate", strategy, type );
							validateConf.addAnnotatedClass( BasicTypesTestEntity.class );
							// The table mapping this entity shouldn't be in the db
							validateConf.addAnnotatedClass( Extra.class );
							return assertThrown( SchemaManagementException.class, setupSessionFactory( validateConf ) )
									.thenAccept( throwable -> assertThat( throwable ).hasMessage( errorMessage ) );
						} )
		);
	}

	/**
	 * An extra entity used for validation,
	 * it should not be created at start up
	 */
	@Entity(name = "Extra")
	@Table(name = Extra.TABLE_NAME)
	public static class Extra {
		public static final String TABLE_NAME = "EXTRA_TABLE";
		@Id
		@GeneratedValue
		private Integer id;

		private String description;
	}

	@Entity(name = "Fruit")
	public static class Fruit {

		@Id
		@GeneratedValue
		private Integer id;

		@Column(name = "something_name", nullable = false, updatable = false)
		private String name;

		public Fruit() {
		}

		public Fruit(String name) {
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return "Fruit{" + id + "," + name + '}';
		}
	}
}
