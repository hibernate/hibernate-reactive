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
import javax.persistence.Table;

import org.hibernate.annotations.Type;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.provider.Settings;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

public abstract class LongVarcharValidationSqlTest extends BaseReactiveTest {

	public static class IndividuallyLongVarcharValidationTestBase
			extends LongVarcharValidationSqlTest {

		@Override
		protected Configuration constructConfiguration(String hbm2DdlOption) {
			final Configuration configuration = super.constructConfiguration( hbm2DdlOption );
			configuration.setProperty( Settings.HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY, INDIVIDUALLY.toString() );
			return configuration;
		}
	}

	public static class GroupedLongVarcharValidationTestBase extends LongVarcharValidationSqlTest {

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
		configuration.addAnnotatedClass( Translation.class );

		test( context, setupSessionFactory( configuration )
				.thenCompose( v -> factoryManager.stop() ) );
	}

	@After
	@Override
	public void after(TestContext context) {
		final Configuration configuration = constructConfiguration( "drop" );
		configuration.addAnnotatedClass( Translation.class );

		test( context, factoryManager.stop()
				.thenCompose( v -> setupSessionFactory( configuration ) )
				.thenCompose( v -> factoryManager.stop() ) );
	}

	@Test
	public void testValidationSucceed(TestContext context) {
		Configuration configuration = constructConfiguration( "validate" );
		configuration.addAnnotatedClass( Translation.class );

		test(
				context,
				setupSessionFactory( configuration )
						.thenCompose( v -> getSessionFactory()
								.withTransaction(
										(session, t) -> session.createQuery( "FROM Translation", Translation.class )
												.getResultList() ) )
						.thenAccept( results -> context.assertTrue( results.isEmpty() ) )
		);
	}

	@Entity(name = "Translation")
	@Table(name = "translation_tbl")
	public static class Translation {
		@Id
		public Integer id;
		@Type(type = "text")
		String text;
	}
}
