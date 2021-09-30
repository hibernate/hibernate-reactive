/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.schema;

import static org.hibernate.tool.schema.JdbcMetadaAccessStrategy.GROUPED;
import static org.hibernate.tool.schema.JdbcMetadaAccessStrategy.INDIVIDUALLY;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.provider.Settings;

import org.junit.After;
import org.junit.Before;

import io.vertx.ext.unit.TestContext;

public abstract class SchemaBaseReactiveTest extends BaseReactiveTest {

	public static class IndividuallySchemaBaseReactiveTestBase extends SchemaBaseReactiveTest {

		@Override
		protected Configuration constructConfiguration(String hbm2DdlOption) {
			final Configuration configuration = super.constructConfiguration( hbm2DdlOption );
			configuration.setProperty( Settings.HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY, INDIVIDUALLY.toString() );
			return configuration;
		}
	}

	public static class GroupedSchemaBaseReactiveTestBase extends SchemaBaseReactiveTest {

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
		return configuration;
	}

	@Before
	@Override
	public void before(TestContext context) {
		Configuration configuration = constructConfiguration( "create" );
		addAnnotatedClasses( configuration );

		test( context, setupSessionFactory( configuration )
				.thenCompose( v -> factoryManager.stop() ) );
	}

	@After
	@Override
	public void after(TestContext context) {
		final Configuration configuration = constructConfiguration( "drop" );
		addAnnotatedClasses( configuration );

		test( context, factoryManager.stop()
				.thenCompose( v -> setupSessionFactory( configuration ) )
				.thenCompose( v -> factoryManager.stop() ) );
	}

	protected void addAnnotatedClasses(Configuration configuration) {
		for( Class clazz : getAnnotatedClasses() ) {
			configuration.addAnnotatedClass(clazz);
		}
	}

	protected Class[] getAnnotatedClasses() {
		return new Class[0];
	}
}
