/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Version;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.MultiTenancyStrategy;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.testing.DatabaseSelectionRule;
import org.hibernate.reactive.util.impl.CompletionStages;

import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.smallrye.mutiny.Uni;
import io.vertx.ext.unit.TestContext;

import static org.hamcrest.CoreMatchers.isA;
import static org.hibernate.reactive.MyCurrentTenantIdentifierResolver.Tenant.DEFAULT;
import static org.hibernate.reactive.MyCurrentTenantIdentifierResolver.Tenant.TENANT_1;
import static org.hibernate.reactive.MyCurrentTenantIdentifierResolver.Tenant.TENANT_2;
import static org.hibernate.reactive.MyCurrentTenantIdentifierResolver.Tenant.values;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;

/**
 * Test Multitenancy without a {@link org.hibernate.context.spi.CurrentTenantIdentifierResolver}.
 * <p>
 * This class creates multiple additional databases so that we can check that queries run
 * on the database for the selected tenant.
 * </p>
 *
 * @see ReactiveMultitenantTest
 */
public class ReactiveMultitenantNoResolverTest extends BaseReactiveTest {

	// To check if we are using the right database we run native queries for PostgreSQL
	@Rule
	public DatabaseSelectionRule selectionRule = DatabaseSelectionRule.runOnlyFor( POSTGRESQL );

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( GuineaPig.class );
		configuration.setProperty( Settings.MULTI_TENANT, MultiTenancyStrategy.DATABASE.name() );
		// Contains the SQL scripts for the creation of the additional databases
		configuration.setProperty( Settings.HBM2DDL_IMPORT_FILES, "/multitenancy-test.sql" );
		configuration.setProperty( Settings.SQL_CLIENT_POOL, TenantDependentPool.class.getName() );
		return configuration;
	}

	@Test
	public void reactivePersistFindDelete(TestContext context) {
		final GuineaPig guineaPig = new GuineaPig( 5, "Aloi" );
		Stage.Session session = getSessionFactory().openSession( DEFAULT.name() );
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
						.thenCompose( session::remove )
						.thenCompose( v -> session.flush() )
						.whenComplete( (v, err) -> session.close() )
		);
	}

	@Test
	public void testWithSessionWithTenant(TestContext context) {
		test( context, getSessionFactory()
				.withSession( TENANT_1.name(), session -> selectCurrentDB( session )
						.thenAccept( result -> context.assertEquals( TENANT_1.getDbName(), result ) ) )
				.thenCompose( unused -> getSessionFactory()
						.withSession( TENANT_2.name(), session -> selectCurrentDB( session )
								.thenAccept( result -> context.assertEquals( TENANT_2.getDbName(), result ) ) ) )
		);
	}

	@Test
	public void testWithSessionWithTenantWithMutiny(TestContext context) {
		test( context, getMutinySessionFactory()
				.withSession( TENANT_1.name(), session -> selectCurrentDB( session )
						.invoke( result -> context.assertEquals( TENANT_1.getDbName(), result ) ) )
				.call( () -> getMutinySessionFactory()
						.withSession( TENANT_2.name(), session -> selectCurrentDB( session )
								.invoke( result -> context.assertEquals( TENANT_2.getDbName(), result ) ) ) )
		);
	}

	@Test
	public void testWithTransactionWithTenant(TestContext context) {
		test( context, getSessionFactory()
				.withTransaction( TENANT_1.name(), (session, tx) -> selectCurrentDB( session )
						.thenAccept( result -> context.assertEquals( TENANT_1.getDbName(), result ) ) )
				.thenCompose( unused -> getSessionFactory()
						.withTransaction( TENANT_2.name(), (session, tx) -> selectCurrentDB( session )
								.thenAccept( result -> context.assertEquals( TENANT_2.getDbName(), result ) ) ) )
		);
	}

	@Test
	public void testWithTransactionWithTenantWithMutiny(TestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( TENANT_1.name(), (session, tx) -> selectCurrentDB( session )
						.invoke( result -> context.assertEquals( TENANT_1.getDbName(), result ) ) )
				.chain( () -> getMutinySessionFactory()
						.withTransaction( TENANT_2.name(), (session, tx) -> selectCurrentDB( session )
								.invoke( result -> context.assertEquals( TENANT_2.getDbName(), result ) ) ) )
		);
	}

	@Test
	public void testOpenSessionWithTenant(TestContext context) {
		Stage.Session t1Session = getSessionFactory().openSession( TENANT_1.name() );
		test( context, selectCurrentDB( t1Session )
				.thenAccept( result -> context.assertEquals( TENANT_1.getDbName(), result ) )
				.whenComplete( (v, e) -> t1Session.close() )
				.thenApply( unused -> getSessionFactory().openSession( TENANT_2.name() ) )
				.thenCompose( t2Session -> selectCurrentDB( t2Session )
						.thenAccept( result -> context.assertEquals( TENANT_2.getDbName(), result ) )
						.whenComplete( (v, e) -> t2Session.close() ) )
		);
	}

	private CompletionStage<Object> selectCurrentDB(Stage.Session session) {
		return session
				.createNativeQuery( "select current_database()" )
				.getSingleResult();
	}

	private Uni<Object> selectCurrentDB(Mutiny.Session session) {
		return session
				.createNativeQuery( "select current_database()" )
				.getSingleResult();
	}

	@Test
	public void testOpenSessionWithTenantWithMutin(TestContext context) {
		Mutiny.Session t1Session = getMutinySessionFactory().openSession( TENANT_1.name() );
		test( context, t1Session
				.createNativeQuery( "select current_database()" )
				.getSingleResult()
				.invoke( result -> context.assertEquals( TENANT_1.getDbName(), result ) )
				.eventually( t1Session::close )
				.replaceWith( () -> getMutinySessionFactory().openSession( TENANT_2.name() ) )
				.call( t2Session -> t2Session
						.createNativeQuery( "select current_database()" )
						.getSingleResult()
						.invoke( result -> context.assertEquals( TENANT_2.getDbName(), result ) )
						.call( t2Session::close ) )
		);
	}

	@Test
	public void testOpenSessionThrowsExceptionWithoutTenant(TestContext context) {
		thrown.expectCause( isA( HibernateException.class ) );
		thrown.expectMessage( "no tenant identifier" );

		test( context, openSession().thenCompose( this::selectCurrentDB ) );
	}

	@Test
	public void testWithSessionThrowsExceptionWithoutTenant(TestContext context) {
		thrown.expectCause( isA( HibernateException.class ) );
		thrown.expectMessage( "no tenant identifier" );

		test( context, getSessionFactory().withSession( this::selectCurrentDB ) );
	}

	@Test
	public void testWithTransactionThrowsExceptionWithoutTenant(TestContext context) {
		thrown.expectCause( isA( HibernateException.class ) );
		thrown.expectMessage( "no tenant identifier" );

		test( context, getSessionFactory().withTransaction( (s, t) -> selectCurrentDB( s ) ) );
	}

	@Test
	public void testOpenSessionThrowsExceptionWithoutTenantWithMutiny(TestContext context) {
		thrown.expect( HibernateException.class );
		thrown.expectMessage( "no tenant identifier" );

		test( context, openMutinySession().call( this::selectCurrentDB ) );
	}

	@Test
	public void testWithSessionThrowsExceptionWithoutTenantWithMutiny(TestContext context) {
		thrown.expect( HibernateException.class );
		thrown.expectMessage( "no tenant identifier" );

		test( context, getMutinySessionFactory().withSession( this::selectCurrentDB ) );
	}

	@Test
	public void testWithTransactionThrowsExceptionWithoutTenantWithMutiny(TestContext context) {
		thrown.expect( HibernateException.class );
		thrown.expectMessage( "no tenant identifier" );

		test( context, getMutinySessionFactory().withTransaction( (s, t) -> selectCurrentDB( s ) ) );
	}

	@AfterClass
	public static void dropDatabases(TestContext context) {
		if ( factoryManager.isStarted() ) {
			test( context, getSessionFactory()
					.withSession( DEFAULT.name(), session -> Arrays
							.stream( values() )
							.filter( tenant -> tenant != DEFAULT )
							.collect(
									CompletionStages::voidFuture,
									(stage, tenant) -> session
											.createNativeQuery( "drop database if exists " + tenant.getDbName() + ";" )
											.executeUpdate()
											.thenCompose( CompletionStages::voidFuture ),
									(stage, stage2) -> stage.thenCompose( v -> stage2 )
							)
					) );
		}
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
