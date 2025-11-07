/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.schema;


import java.net.URL;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.annotations.DisabledFor;
import org.hibernate.reactive.annotations.EnabledFor;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.tool.schema.spi.SchemaManagementException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.hibernate.tool.schema.JdbcMetadaAccessStrategy.GROUPED;
import static org.hibernate.tool.schema.JdbcMetadaAccessStrategy.INDIVIDUALLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test schema validation at startup for all the supported types:
 * - Missing table validation error
 * - No validation error when everything is fine
 * - TODO: Missing column
 * - TODO: Wrong column type
 */
@DisabledFor(value = DB2, reason = "We don't have an information extractor. See https://github.com/hibernate/hibernate-reactive/issues/911")
public abstract class SchemaValidationTestBase extends BaseReactiveTest {

	public static class IndividuallyStrategyTest extends SchemaValidationTestBase {

		@Override
		protected Configuration constructConfiguration(String hbm2DdlOption) {
			final Configuration configuration = super.constructConfiguration( hbm2DdlOption );
			configuration.setProperty( Settings.HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY, INDIVIDUALLY.toString() );
			return configuration;
		}
	}

	public static class GroupedStrategyTest extends SchemaValidationTestBase {

		@Override
		protected Configuration constructConfiguration(String hbm2DdlOption) {
			final Configuration configuration = super.constructConfiguration( hbm2DdlOption );
			configuration.setProperty( Settings.HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY, GROUPED.toString() );
			return configuration;
		}
	}

	protected Configuration constructConfiguration(String action) {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( Settings.HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY, INDIVIDUALLY.toString() );
		configuration.setProperty( Settings.HBM2DDL_AUTO, action );
		return configuration;
	}

	@BeforeEach
	@Override
	public void before(VertxTestContext context) {
		Configuration createConf = constructConfiguration( "create" );
		createConf.addAnnotatedClass( BasicTypesTestEntity.class );

		final URL importFileURL = Thread.currentThread()
				.getContextClassLoader()
				.getResource( "oracle-SchemaValidationTest.sql" );
		createConf.setProperty( AvailableSettings.JAKARTA_HBM2DDL_LOAD_SCRIPT_SOURCE, importFileURL.getFile() );

		// Make sure that the extra table is not in the db
		Configuration dropConf = constructConfiguration( "drop" );
		dropConf.addAnnotatedClass( Extra.class );

		test( context, setupSessionFactory( dropConf )
				.thenCompose( v -> factoryManager.stop() )
				.thenCompose( v -> setupSessionFactory( createConf ) )
				.thenCompose( v -> factoryManager.stop() )
		);
	}

	@AfterEach
	@Override
	public void after(VertxTestContext context) {
		super.after( context );
		closeFactory( context );
	}

	@Test
	@Timeout(value = 10, timeUnit = MINUTES)
	@EnabledFor( ORACLE )
	public void testOracleColumnTypeValidation(VertxTestContext context) {
		Configuration validateConf = constructConfiguration( "validate" );
		validateConf.addAnnotatedClass( Fruit.class );

		StandardServiceRegistryBuilder builder = new StandardServiceRegistryBuilder()
				.applySettings( validateConf.getProperties() );
		test( context, setupSessionFactory( validateConf ) );
	}

	// When we have created the table, the validation should pass
	@Test
	@Timeout(value = 10, timeUnit = MINUTES)
	public void testValidationSucceeds(VertxTestContext context) {
		Configuration validateConf = constructConfiguration( "validate" );
		validateConf.addAnnotatedClass( BasicTypesTestEntity.class );

		StandardServiceRegistryBuilder builder = new StandardServiceRegistryBuilder()
				.applySettings( validateConf.getProperties() );
		test( context, setupSessionFactory( validateConf ) );
	}


	// Validation should fail if a table is missing
	@Test
	@Timeout(value = 10, timeUnit = MINUTES)
	public void testValidationFails(VertxTestContext context) {
		Configuration validateConf = constructConfiguration( "validate" );
		validateConf.addAnnotatedClass( BasicTypesTestEntity.class );
		// The table mapping this entity shouldn't be in the db
		validateConf.addAnnotatedClass( Extra.class );

		final String errorMessage = "Schema-validation: missing table [" + Extra.TABLE_NAME + "]";
		test( context, setupSessionFactory( validateConf )
				.handle( (unused, throwable) -> {
					assertNotNull( throwable );
					assertEquals( throwable.getClass(), SchemaManagementException.class );
					assertEquals( throwable.getMessage(), errorMessage );
					return null;
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
