/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.testing.DBSelectionExtension;
import org.hibernate.reactive.testing.SqlStatementTracker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.hibernate.reactive.testing.DBSelectionExtension.runOnlyFor;

/**
 * The processing of the query is done by Hibernate ORM in {@link org.hibernate.reactive.query.sql.internal.ReactiveNativeSelectQueryPlanImpl}
 * via the {@link org.hibernate.query.sql.internal.SQLQueryParser} for all databases.
 * <p>
 *     We are only testing this only on PostgreSQL, so that we can keep it simple.
 *     We might add the other databases when necessary
 * </p>
 *
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class NativeQueryPlaceholderSubstitutionTest extends BaseReactiveTest {

	@RegisterExtension
	public DBSelectionExtension dbRule = runOnlyFor( POSTGRESQL );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Widget.class );
	}

	private static SqlStatementTracker sqlStatementTracker;

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( Settings.DEFAULT_SCHEMA, "public" );
		sqlStatementTracker = new SqlStatementTracker(
				NativeQueryPlaceholderSubstitutionTest::isSelectOrUpdate,
				configuration.getProperties()
		);
		return configuration;
	}

	@Override
	protected void addServices(StandardServiceRegistryBuilder builder) {
		sqlStatementTracker.registerService( builder );
	}

	private static boolean isSelectOrUpdate(String sql) {
		return sql.startsWith( "select count" ) || sql.startsWith( "update " );
	}

	@Test
	public void testSchemaPlaceHolderSubstitution(VertxTestContext context) {
		sqlStatementTracker.clear();
		test( context, getSessionFactory()
				.withSession( session -> session
						.createNativeQuery( "select count(*) from {h-schema}widgets", Integer.class )
						.getSingleResult()
						.thenAccept( result -> assertThat( result ).isZero() ) )
				.thenCompose( v -> getSessionFactory().withSession( session -> session
						.createNativeQuery( "update {h-schema}widgets set id = 1" )
						.executeUpdate()
						.thenAccept( result -> assertThat( result ).isZero() ) ) )
				.thenAccept( v -> assertThat( sqlStatementTracker.getLoggedQueries() )
						.containsExactly( "select count(*) from public.widgets", "update public.widgets set id = 1" ) )
		);
	}

	@Test
	public void testDomainPlaceHolderSubstitution(VertxTestContext context) {
		sqlStatementTracker.clear();
		test( context, getSessionFactory()
				.withSession( session -> session
						.createNativeQuery( "select count(*) from {h-domain}widgets", Integer.class )
						.getSingleResult()
						.thenAccept( result -> assertThat( result ).isZero() ) )
				.thenCompose( v -> getSessionFactory().withSession( session -> session
						.createNativeQuery( "update {h-domain}widgets set id = 1" )
						.executeUpdate()
						.thenAccept( result -> assertThat( result ).isZero() ) ) )
				.thenAccept( v -> assertThat( sqlStatementTracker.getLoggedQueries() )
						.containsExactly( "select count(*) from public.widgets", "update public.widgets set id = 1" ) )
		);
	}

	@Test
	public void testCatalogPlaceHolderSubstitution(VertxTestContext context) {
		sqlStatementTracker.clear();
		test( context, getSessionFactory()
				.withSession( session -> session
						.createNativeQuery( "select count(*) from {h-catalog}widgets", Integer.class )
						.getSingleResult()
						.thenAccept( result -> assertThat( result ).isZero() ) )
				.thenCompose( v -> getSessionFactory().withSession( session -> session
						.createNativeQuery( "update {h-catalog}widgets set id = 1" )
						.executeUpdate()
						.thenAccept( result -> assertThat( result ).isZero() ) ) )
				// PostgreSQL uses the schema, so that catalog property is null
				.thenAccept( v -> assertThat( sqlStatementTracker.getLoggedQueries() )
						.containsExactly( "select count(*) from widgets", "update widgets set id = 1" ) )
		);
	}

	@Entity(name = "Widget")
	@Table(name = "widgets")
	public static class Widget {
		@Id
		@GeneratedValue
		private Long id;
	}
}
