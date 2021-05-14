/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Objects;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Version;

import org.hibernate.LockMode;
import org.hibernate.MultiTenancyStrategy;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.hibernate.reactive.MyCurrentTenantIdentifierResolver.Tenant.DEFAULT;
import static org.hibernate.reactive.MyCurrentTenantIdentifierResolver.Tenant.TENANT_1;
import static org.hibernate.reactive.MyCurrentTenantIdentifierResolver.Tenant.TENANT_2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;

/**
 * This class creates multiple additional databases so that we can check that queries run
 * on the database for the selected tenant.
 */
public class ReactiveMultitenantTest extends BaseReactiveTest {

	private static final MyCurrentTenantIdentifierResolver TENANT_RESOLVER = new MyCurrentTenantIdentifierResolver();

	// To check if we are using the right database we run native queries for PostgreSQL
	@Rule
	public DatabaseSelectionRule selectionRule = DatabaseSelectionRule.runOnlyFor( POSTGRESQL );

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( GuineaPig.class );
		configuration.setProperty( Settings.MULTI_TENANT, MultiTenancyStrategy.DATABASE.name() );
		configuration.getProperties().put( Settings.MULTI_TENANT_IDENTIFIER_RESOLVER, TENANT_RESOLVER );
		// Contains the SQL scripts for the creation of the additional databases
		configuration.setProperty( Settings.HBM2DDL_IMPORT_FILES, "/multitenancy-test.sql" );
		configuration.setProperty( Settings.SQL_CLIENT_POOL, TenantDependentPool.class.getName() );
		return configuration;
	}

	@Test
	public void reactivePersistFindDelete(TestContext context) {
		TENANT_RESOLVER.setTenantIdentifier( DEFAULT );
		final GuineaPig guineaPig = new GuineaPig( 5, "Aloi" );
		Stage.Session session = getSessionFactory().openSession();
		test(
				context,
				session.persist( guineaPig )
						.thenCompose( v -> session.flush() )
						.thenAccept( v -> session.detach( guineaPig ) )
						.thenAccept( v -> context.assertFalse( session.contains( guineaPig ) ) )
						.thenCompose( v -> session.find( GuineaPig.class, guineaPig.getId() ) )
						.thenAccept( actualPig -> {
							assertThatPigsAreEqual( context, guineaPig, actualPig );
							context.assertTrue( session.contains( actualPig ) );
							context.assertFalse( session.contains( guineaPig ) );
							context.assertEquals( LockMode.READ, session.getLockMode( actualPig ) );
							session.detach( actualPig );
							context.assertFalse( session.contains( actualPig ) );
						} )
						.thenCompose( v -> session.find( GuineaPig.class, guineaPig.getId() ) )
						.thenCompose( pig -> session.remove( pig ) )
						.thenCompose( v -> session.flush() )
						.whenComplete( (v, err) -> session.close() )
		);
	}

	@Test
	public void testTenantSelection(TestContext context) {
		TENANT_RESOLVER.setTenantIdentifier( TENANT_1 );
		test( context, openSession()
				.thenCompose( session -> session
						.createNativeQuery( "select current_database()" )
						.getSingleResult()
						.thenAccept( result -> context.assertEquals( TENANT_1.getDbName(), result ) ) )
				.thenAccept( unused -> TENANT_RESOLVER.setTenantIdentifier( TENANT_2 ) )
				.thenCompose( unused -> openSession() )
				.thenCompose( session -> session
						.createNativeQuery( "select current_database()" )
						.getSingleResult()
						.thenAccept( result -> context.assertEquals( TENANT_2.getDbName(), result ) ) )
		);
	}

	private void assertThatPigsAreEqual(TestContext context, GuineaPig expected, GuineaPig actual) {
		context.assertNotNull( actual );
		context.assertEquals( expected.getId(), actual.getId() );
		context.assertEquals( expected.getName(), actual.getName() );
	}

	@Entity(name = "GuineaPig")
	@Table(name = "Pig")
	public static class GuineaPig {
		@Id
		private Integer id;
		private String name;
		@Version
		private int version;

		public GuineaPig() {
		}

		public GuineaPig(Integer id, String name) {
			this.id = id;
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
			return id + ": " + name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			GuineaPig guineaPig = (GuineaPig) o;
			return Objects.equals( name, guineaPig.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}
	}

}
