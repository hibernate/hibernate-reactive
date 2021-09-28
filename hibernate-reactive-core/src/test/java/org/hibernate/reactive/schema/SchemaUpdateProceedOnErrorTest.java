/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.schema;

import static org.hibernate.tool.schema.JdbcMetadaAccessStrategy.GROUPED;
import static org.hibernate.tool.schema.JdbcMetadaAccessStrategy.INDIVIDUALLY;

import javax.persistence.Entity;
import javax.persistence.Id;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.provider.Settings;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

public class SchemaUpdateProceedOnErrorTest extends BaseReactiveTest {

	public static class IndividuallySchemaUpdateSchemaNameDB2TestBase extends SchemaUpdateProceedOnErrorTest {

		@Override
		protected Configuration constructConfiguration(String hbm2DdlOption) {
			final Configuration configuration = super.constructConfiguration( hbm2DdlOption );
			configuration.setProperty( Settings.HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY, INDIVIDUALLY.toString() );
			return configuration;
		}
	}

	public static class GroupedSchemaUpdateSchemaNameTestBase extends SchemaUpdateProceedOnErrorTest {

		@Override
		protected Configuration constructConfiguration(String hbm2DdlOption) {
			final Configuration configuration = super.constructConfiguration( hbm2DdlOption );
			configuration.setProperty( Settings.HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY, GROUPED.toString() );
			return configuration;
		}
	}

	protected Configuration constructConfiguration(String hbm2DdlOption) {
		Configuration configuration = constructConfiguration();
		configuration.setProperty( Settings.HBM2DDL_AUTO, hbm2DdlOption );
		configuration.setProperty( Settings.DEFAULT_CATALOG, "hreact" );
		return configuration;
	}

	@Before
	@Override
	public void before(TestContext context) {
		Configuration configuration = constructConfiguration( "create" );
		configuration.addAnnotatedClass( From.class );

		test( context, setupSessionFactory( configuration )
				.thenCompose( v -> factoryManager.stop() ) );
	}

	@After
	@Override
	public void after(TestContext context) {
		final Configuration configuration = constructConfiguration( "drop" );
		configuration.addAnnotatedClass( From.class );

		test( context, factoryManager.stop()
				.thenCompose( v -> setupSessionFactory( configuration ) )
				.thenCompose( v -> factoryManager.stop() ) );
	}

	@Test
	public void testHaltOnError(TestContext context) {
		final Configuration updateHbm2ddlConf = constructConfiguration( "update" );

		test(
				context,
				setupSessionFactory( updateHbm2ddlConf )
						.thenAccept( v -> getSessionFactory() )

		);
	}

	@Entity(name = "From")
	public class From {

		@Id
		private Integer id;

		private String table;

		private String select;
	}
}
