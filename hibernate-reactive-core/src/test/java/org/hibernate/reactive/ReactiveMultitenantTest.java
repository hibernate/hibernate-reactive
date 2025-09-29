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
import org.hibernate.reactive.annotations.EnabledFor;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.MyCurrentTenantIdentifierResolver.Tenant.DEFAULT;
import static org.hibernate.reactive.MyCurrentTenantIdentifierResolver.Tenant.TENANT_1;
import static org.hibernate.reactive.MyCurrentTenantIdentifierResolver.Tenant.TENANT_2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;

/**
 * This class creates multiple additional databases so that we can check that queries run
 * on the database for the selected tenant.
 */
@Timeout(value = 10, timeUnit = MINUTES)
@EnabledFor(value = POSTGRESQL, reason = "Native queries for this test are targeted for PostgreSQL")
public class ReactiveMultitenantTest extends BaseReactiveTest {

	private static final MyCurrentTenantIdentifierResolver TENANT_RESOLVER = new MyCurrentTenantIdentifierResolver();

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( GuineaPig.class );
		// FIXME this is terrible?
		configuration.setProperty( AvailableSettings.MULTI_TENANT_CONNECTION_PROVIDER, "anything" );
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
		test( context, openSession()
				.thenCompose( session -> session
						.persist( guineaPig )
						.thenCompose( v -> session.flush() )
						.thenAccept( v -> session.detach( guineaPig ) )
						.thenAccept( v -> assertThat( session.contains( guineaPig ) ).isFalse() )
						.thenCompose( v -> session.find( GuineaPig.class, guineaPig.getId() ) )
						.thenAccept( actualPig -> {
							assertThat( actualPig ).isNotNull();
							assertThat( actualPig.getId() ).isEqualTo( guineaPig.getId() );
							assertThat( actualPig.getName() ).isEqualTo( guineaPig.getName() );
							assertThat( session.contains( actualPig ) ).isTrue();
							assertThat( session.contains( guineaPig ) ).isFalse();
							assertThat( session.getLockMode( actualPig ) ).isEqualTo( LockMode.READ );
							session.detach( actualPig );
							assertThat( session.contains( actualPig ) ).isFalse();
						} )
						.thenCompose( v -> session.find( GuineaPig.class, guineaPig.getId() ) )
						.thenCompose( session::remove )
						.thenCompose( v -> session.flush() )
				)
		);
	}

	@Test
	public void testTenantSelection(VertxTestContext context) {
		TENANT_RESOLVER.setTenantIdentifier( TENANT_1 );
		test( context, openSession()
				.thenCompose( session -> session
						.createNativeQuery( "select current_database()" )
						.getSingleResult()
						.thenAccept( result -> assertThat( result ).isEqualTo( TENANT_1.getDbName() ) ) )
				.thenAccept( unused -> TENANT_RESOLVER.setTenantIdentifier( TENANT_2 ) )
				.thenCompose( unused -> openSession() )
				.thenCompose( session -> session
						.createNativeQuery( "select current_database()" )
						.getSingleResult()
						.thenAccept( result -> assertThat( result ).isEqualTo( TENANT_2.getDbName() ) ) )
		);
	}

	@Test
	public void testTenantSelectionStatelessSession(VertxTestContext context) {
		TENANT_RESOLVER.setTenantIdentifier( TENANT_1 );
		test( context, getSessionFactory().openStatelessSession()
				.thenCompose( t1Session -> t1Session
					.createNativeQuery( "select current_database()" )
					.getSingleResult()
					.thenAccept( result -> assertThat( result ).isEqualTo( TENANT_1.getDbName() ) )
					.thenCompose( unused -> t1Session.close() ) )
				.thenAccept( unused -> TENANT_RESOLVER.setTenantIdentifier( TENANT_2 ) )
				.thenCompose( v -> getSessionFactory().openStatelessSession() )
				.thenCompose( t2Session -> t2Session
						.createNativeQuery( "select current_database()" )
						.getSingleResult()
						.thenAccept( result -> assertThat( result ).isEqualTo( TENANT_2.getDbName() ) )
						.thenCompose( v -> t2Session.close() ) )
		);
	}

	@Test
	public void testTenantSelectionStatelessSessionMutiny(VertxTestContext context) {
		TENANT_RESOLVER.setTenantIdentifier( TENANT_1 );
		test( context, getMutinySessionFactory()
				.withStatelessSession( t1Session -> t1Session
						.createNativeQuery( "select current_database()" )
						.getSingleResult()
						.invoke( result -> assertThat( result ).isEqualTo( TENANT_1.getDbName() ) )
				)
				.invoke( result -> TENANT_RESOLVER.setTenantIdentifier( TENANT_2 ) )
				.chain( () -> getMutinySessionFactory().withStatelessSession( t2Session -> t2Session
						.createNativeQuery( "select current_database()" )
						.getSingleResult()
						.invoke( result -> assertThat( result ).isEqualTo( TENANT_2.getDbName() ) )
				) )
		);
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
