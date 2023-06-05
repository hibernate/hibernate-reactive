/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Objects;

import org.hibernate.LockMode;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.testing.DBSelectionExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hibernate.reactive.MyCurrentTenantIdentifierResolver.Tenant.DEFAULT;
import static org.hibernate.reactive.MyCurrentTenantIdentifierResolver.Tenant.TENANT_1;
import static org.hibernate.reactive.MyCurrentTenantIdentifierResolver.Tenant.TENANT_2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This class creates multiple additional databases so that we can check that queries run
 * on the database for the selected tenant.
 */
@Timeout(value = 10, timeUnit = MINUTES)

public class ReactiveMultitenantTest extends BaseReactiveTest {

	private static final MyCurrentTenantIdentifierResolver TENANT_RESOLVER = new MyCurrentTenantIdentifierResolver();

	// To check if we are using the right database we run native queries for PostgreSQL
	@RegisterExtension
	public DBSelectionExtension selectionRule = DBSelectionExtension.runOnlyFor( POSTGRESQL );

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( GuineaPig.class );
		configuration.setProperty(
				AvailableSettings.MULTI_TENANT_CONNECTION_PROVIDER,
				"anything"
		);//FIXME this is terrible?
		configuration.getProperties().put( Settings.MULTI_TENANT_IDENTIFIER_RESOLVER, TENANT_RESOLVER );
		// Contains the SQL scripts for the creation of the additional databases
		configuration.setProperty( Settings.HBM2DDL_IMPORT_FILES, "/multitenancy-test.sql" );
		configuration.setProperty( Settings.SQL_CLIENT_POOL, TenantDependentPool.class.getName() );
		return configuration;
	}

	@Test
	public void reactivePersistFindDelete(VertxTestContext context) {
		TENANT_RESOLVER.setTenantIdentifier( DEFAULT );
		final GuineaPig guineaPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				getSessionFactory().openSession().thenCompose( session -> session
						.persist( guineaPig )
						.thenCompose( v -> session.flush() )
						.thenAccept( v -> session.detach( guineaPig ) )
						.thenAccept( v -> assertFalse( session.contains( guineaPig ) ) )
						.thenCompose( v -> session.find( GuineaPig.class, guineaPig.getId() ) )
						.thenAccept( actualPig -> {
							assertThatPigsAreEqual( guineaPig, actualPig );
							assertTrue( session.contains( actualPig ) );
							assertFalse( session.contains( guineaPig ) );
							assertEquals( LockMode.READ, session.getLockMode( actualPig ) );
							session.detach( actualPig );
							assertFalse( session.contains( actualPig ) );
						} )
						.thenCompose( v -> session.find( GuineaPig.class, guineaPig.getId() ) )
						.thenCompose( session::remove )
						.thenCompose( v -> session.flush() ) )
		);
	}

	@Test
	public void testTenantSelection(VertxTestContext context) {
		TENANT_RESOLVER.setTenantIdentifier( TENANT_1 );
		test( context, openSession()
				.thenCompose( session -> session
						.createNativeQuery( "select current_database()" )
						.getSingleResult()
						.thenAccept( result -> assertEquals( TENANT_1.getDbName(), result ) ) )
				.thenAccept( unused -> TENANT_RESOLVER.setTenantIdentifier( TENANT_2 ) )
				.thenCompose( unused -> openSession() )
				.thenCompose( session -> session
						.createNativeQuery( "select current_database()" )
						.getSingleResult()
						.thenAccept( result -> assertEquals( TENANT_2.getDbName(), result ) ) )
		);
	}

	@Test
	public void testTenantSelectionStatelessSession(VertxTestContext context) {
		TENANT_RESOLVER.setTenantIdentifier( TENANT_1 );
		test( context, getSessionFactory().openStatelessSession()
				.thenCompose( t1Session -> t1Session
					.createNativeQuery( "select current_database()" )
					.getSingleResult()
					.thenAccept( result -> assertEquals( TENANT_1.getDbName(), result ) )
					.thenCompose( unused -> t1Session.close() ) )
				.thenAccept( unused -> TENANT_RESOLVER.setTenantIdentifier( TENANT_2 ) )
				.thenCompose( v -> getSessionFactory().openStatelessSession() )
				.thenCompose( t2Session -> t2Session
						.createNativeQuery( "select current_database()" )
						.getSingleResult()
						.thenAccept( result -> assertEquals( TENANT_2.getDbName(), result ) )
						.thenCompose( v -> t2Session.close() ) )
		);
	}

	@Test
	public void testTenantSelectionStatelessSessionMutiny(VertxTestContext context) {
		TENANT_RESOLVER.setTenantIdentifier( TENANT_1 );
		test( context, getMutinySessionFactory().withStatelessSession( t1Session ->
				t1Session
				.createNativeQuery( "select current_database()" )
				.getSingleResult()
				.invoke( result -> assertEquals( TENANT_1.getDbName(), result ) )
			  )
				.invoke( result -> TENANT_RESOLVER.setTenantIdentifier( TENANT_2 ) )
				.chain( () -> getMutinySessionFactory().withStatelessSession( t2Session -> t2Session
						.createNativeQuery( "select current_database()" )
						.getSingleResult()
						.invoke( result -> assertEquals( TENANT_2.getDbName(), result ) ) ) )
		);
	}

	private void assertThatPigsAreEqual( GuineaPig expected, GuineaPig actual) {
		assertNotNull( actual );
		assertEquals( expected.getId(), actual.getId() );
		assertEquals( expected.getName(), actual.getName() );
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
