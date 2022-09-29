/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.hibernate.annotations.ColumnDefault;
import org.hibernate.annotations.Generated;
import org.hibernate.annotations.GenerationTime;
import org.hibernate.annotations.GeneratorType;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.COCKROACHDB;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MYSQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.hibernate.reactive.testing.DatabaseSelectionRule.runOnlyFor;

/**
 * Test the @{@link Generated} annotation
 */
public class GeneratedPropertySingleTableTest extends BaseReactiveTest {

	// It requires native queries, so it won't work for every db
	@Rule
	public DatabaseSelectionRule selectionRule = runOnlyFor( POSTGRESQL, COCKROACHDB, MYSQL );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( GeneratedWithIdentity.class, GeneratedRegular.class );
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty(AvailableSettings.HBM2DDL_CREATE_SOURCE, "script-then-metadata");
		configuration.setProperty(AvailableSettings.HBM2DDL_CREATE_SCRIPT_SOURCE, "/mysql-pipe.sql");
		return configuration;
	}

	@Test
	public void testWithIdentity(TestContext context) {
		final GeneratedWithIdentity davide = new GeneratedWithIdentity( "Davide", "D'Alto" );

		CurrentUser.INSTANCE.logIn( "dd-insert" );
		test(
				context,
				getMutinySessionFactory()
						// Generated during insert
						.withSession( session -> session.persist( davide ).call( session::flush )
								.invoke( v -> {
									context.assertNotNull( davide.id );
									context.assertEquals( "Davide", davide.firstname );
									context.assertEquals( "D'Alto", davide.lastname );
									context.assertEquals( "Davide D'Alto", davide.fullName );
									context.assertNotNull( davide.createdAt );
									context.assertEquals( "dd-insert", davide.createdBy );
									context.assertEquals( "dd-insert", davide.updatedBy );
									context.assertNull( davide.never );
									CurrentUser.INSTANCE.logOut();
								} ) )
						// Generated during update
						.chain( () -> getMutinySessionFactory()
								.withSession( session -> session.find( GeneratedWithIdentity.class, davide.id )
										.chain( result -> {
											CurrentUser.INSTANCE.logIn( "dd-update" );
											result.lastname = "O'Tall";
											return session.flush().invoke( afterUpdate -> {
												context.assertEquals( "Davide", result.firstname );
												context.assertEquals( "O'Tall", result.lastname );
												context.assertEquals( "Davide O'Tall", result.fullName );
												context.assertEquals( davide.createdAt, result.createdAt );
												context.assertEquals( "dd-insert", result.createdBy );
												context.assertEquals( "dd-update", result.updatedBy );
												context.assertNull( result.never );
												CurrentUser.INSTANCE.logOut();
											} );
										} ) ) )
		);
	}

	@Test
	public void testRegularEntity(TestContext context) {
		final GeneratedRegular davide = new GeneratedRegular( "Davide", "D'Alto" );

		CurrentUser.INSTANCE.logIn( "dd-insert" );
		test(
				context,
				getMutinySessionFactory()
						// Generated during insert
						.withSession( session -> session.persist( davide ).call( session::flush )
								.invoke( v -> {
									context.assertNotNull( davide.id );
									context.assertEquals( "Davide", davide.firstname );
									context.assertEquals( "D'Alto", davide.lastname );
									context.assertEquals( "Davide D'Alto", davide.fullName );
									context.assertNotNull( davide.createdAt );
									context.assertEquals( "dd-insert", davide.createdBy );
									context.assertEquals( "dd-insert", davide.updatedBy );
									context.assertNull( davide.never );
									CurrentUser.INSTANCE.logOut();
								} ) )
						// Generated during update
						.chain( () -> getMutinySessionFactory()
								.withSession( session -> session.find( GeneratedRegular.class, davide.id )
										.chain( result -> {
											CurrentUser.INSTANCE.logIn( "dd-update" );
											result.lastname = "O'Tall";
											return session.flush().invoke( afterUpdate -> {
												context.assertEquals( "Davide", result.firstname );
												context.assertEquals( "O'Tall", result.lastname );
												context.assertEquals( "Davide O'Tall", result.fullName );
												context.assertEquals( davide.createdAt, result.createdAt );
												context.assertEquals( "dd-insert", result.createdBy );
												context.assertEquals( "dd-update", result.updatedBy );
												context.assertNull( result.never );
												CurrentUser.INSTANCE.logOut();
											} );
										} ) ) )
		);
	}

	@Entity
	@Table(name = "GeneratedRegularSingleTable")
	static class GeneratedRegular {
		@Id
		@GeneratedValue(strategy = GenerationType.AUTO)
		public Long id;

		public String firstname;

		public String lastname;

		@Generated(GenerationTime.ALWAYS)
		@Column(columnDefinition = "varchar(600) generated always as (firstname || ' ' || lastname) stored")
		private String fullName;

		@Temporal(value = TemporalType.TIMESTAMP)
		@Generated(GenerationTime.INSERT)
		@Column(columnDefinition = "timestamp")
		@ColumnDefault("current_timestamp")
		public Date createdAt;

		@GeneratorType(type = CurrentUser.LoggedUserGeneratorWithMutiny.class, when = GenerationTime.INSERT)
		public String createdBy;

		@GeneratorType(type = CurrentUser.LoggedUserGeneratorWithStage.class, when = GenerationTime.ALWAYS)
		public String updatedBy;

		@Generated(GenerationTime.NEVER)
		public String never;

		public GeneratedRegular() {
		}

		public GeneratedRegular(String firstname, String lastname) {
			this.firstname = firstname;
			this.lastname = lastname;
		}
	}

	@Entity
	@Table(name = "GeneratedWithIdentitySingleTable")
	static class GeneratedWithIdentity {
		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		public Long id;

		public String firstname;

		public String lastname;

		@Generated(GenerationTime.ALWAYS)
		@Column(columnDefinition = "varchar(600) generated always as (firstname || ' ' || lastname) stored")
		private String fullName;

		@Temporal(value = TemporalType.TIMESTAMP)
		@Generated(GenerationTime.INSERT)
		@Column(columnDefinition = "timestamp")
		@ColumnDefault("current_timestamp")
		public Date createdAt;

		@GeneratorType(type = CurrentUser.LoggedUserGeneratorWithMutiny.class, when = GenerationTime.INSERT)
		public String createdBy;

		@GeneratorType(type = CurrentUser.LoggedUserGeneratorWithStage.class, when = GenerationTime.ALWAYS)
		public String updatedBy;

		@Generated(GenerationTime.NEVER)
		public String never;

		public GeneratedWithIdentity() {
		}

		public GeneratedWithIdentity(String firstname, String lastname) {
			this.firstname = firstname;
			this.lastname = lastname;
		}
	}
}
