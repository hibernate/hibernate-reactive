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
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.testing.DBSelectionExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.COCKROACHDB;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MYSQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.hibernate.reactive.testing.DBSelectionExtension.runOnlyFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Test the @{@link Generated} annotation
 */
@Timeout(value = 10, timeUnit = MINUTES)

public class GeneratedPropertySingleTableTest extends BaseReactiveTest {

	// It requires native queries, so it won't work for every db
	@RegisterExtension
	public DBSelectionExtension selectionRule = runOnlyFor( POSTGRESQL, COCKROACHDB, MYSQL );

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
	public void testWithIdentity(VertxTestContext context) {
		final GeneratedWithIdentity davide = new GeneratedWithIdentity( "Davide", "D'Alto" );

		CurrentUser.INSTANCE.logIn( "dd-insert" );
		test(
				context,
				getMutinySessionFactory()
						// Generated during insert
						.withSession( session -> session.persist( davide ).call( session::flush )
								.invoke( v -> {
									assertNotNull( davide.id );
									assertEquals( "Davide", davide.firstname );
									assertEquals( "D'Alto", davide.lastname );
									assertEquals( "Davide D'Alto", davide.fullName );
									assertNotNull( davide.createdAt );
									assertEquals( "dd-insert", davide.createdBy );
									assertEquals( "dd-insert", davide.updatedBy );
									assertNull( davide.never );
									CurrentUser.INSTANCE.logOut();
								} ) )
						// Generated during update
						.chain( () -> getMutinySessionFactory()
								.withSession( session -> session.find( GeneratedWithIdentity.class, davide.id )
										.chain( result -> {
											CurrentUser.INSTANCE.logIn( "dd-update" );
											result.lastname = "O'Tall";
											return session.flush().invoke( afterUpdate -> {
												assertEquals( "Davide", result.firstname );
												assertEquals( "O'Tall", result.lastname );
												assertEquals( "Davide O'Tall", result.fullName );
												assertEquals( davide.createdAt, result.createdAt );
												assertEquals( "dd-insert", result.createdBy );
												assertEquals( "dd-update", result.updatedBy );
												assertNull( result.never );
												CurrentUser.INSTANCE.logOut();
											} );
										} ) ) )
		);
	}

	@Test
	public void testRegularEntity(VertxTestContext context) {
		final GeneratedRegular davide = new GeneratedRegular( "Davide", "D'Alto" );

		CurrentUser.INSTANCE.logIn( "dd-insert" );
		test(
				context,
				getMutinySessionFactory()
						// Generated during insert
						.withSession( session -> session.persist( davide ).call( session::flush )
								.invoke( v -> {
									assertNotNull( davide.id );
									assertEquals( "Davide", davide.firstname );
									assertEquals( "D'Alto", davide.lastname );
									assertEquals( "Davide D'Alto", davide.fullName );
									assertNotNull( davide.createdAt );
									assertEquals( "dd-insert", davide.createdBy );
									assertEquals( "dd-insert", davide.updatedBy );
									assertNull( davide.never );
									CurrentUser.INSTANCE.logOut();
								} ) )
						// Generated during update
						.chain( () -> getMutinySessionFactory()
								.withSession( session -> session.find( GeneratedRegular.class, davide.id )
										.chain( result -> {
											CurrentUser.INSTANCE.logIn( "dd-update" );
											result.lastname = "O'Tall";
											return session.flush().invoke( afterUpdate -> {
												assertEquals( "Davide", result.firstname );
												assertEquals( "O'Tall", result.lastname );
												assertEquals( "Davide O'Tall", result.fullName );
												assertEquals( davide.createdAt, result.createdAt );
												assertEquals( "dd-insert", result.createdBy );
												assertEquals( "dd-update", result.updatedBy );
												assertNull( result.never );
												CurrentUser.INSTANCE.logOut();
											} );
										} ) ) )
		);
	}

	@Entity(name = "GeneratedRegular")
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

		@CurrentUser.LoggedUserMutinyInsert
		public String createdBy;

		@CurrentUser.LoggedUserStageAlways
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

	@Entity(name = "GeneratedWithIdentity")
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

		@CurrentUser.LoggedUserMutinyInsert
		public String createdBy;

		@CurrentUser.LoggedUserStageAlways
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
