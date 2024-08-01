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
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.generator.EventType;
import org.hibernate.reactive.annotations.DisabledFor;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Inheritance;
import jakarta.persistence.InheritanceType;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.SQLSERVER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Test the @{@link Generated} annotation with {@link InheritanceType#TABLE_PER_CLASS}
 * <p>
 * Note that id generation using Identity is not a valid mapping.
 * </p>
 */
@Timeout(value = 10, timeUnit = MINUTES)
@DisabledFor(value = {SQLSERVER, ORACLE, DB2}, reason = "test uses SQL queries")
public class GeneratedPropertyUnionSubclassesTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( GeneratedRegular.class );
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( AvailableSettings.HBM2DDL_CREATE_SOURCE, "script-then-metadata" );
		configuration.setProperty( AvailableSettings.HBM2DDL_CREATE_SCRIPT_SOURCE, "/mysql-pipe.sql" );
		return configuration;
	}

	@Test
	public void testRegularEntity(VertxTestContext context) {
		final GeneratedRegular davide = new GeneratedRegular( "Davide", "D'Alto" );

		CurrentUser.INSTANCE.logIn( "dd-insert" );
		test( context, getMutinySessionFactory()
				// Generated during insert
				.withTransaction( session -> session.persist( davide ) )
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
				} )
				// Generated during update
				.chain( () -> getMutinySessionFactory()
						.withTransaction( session -> session.find( GeneratedRegular.class, davide.id )
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

	@Entity(name = "GeneratedRegularParent")
	@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
	static class GeneratedRegularParent {
		@Id
		@GeneratedValue(strategy = GenerationType.AUTO)
		public Long id;

		public String firstname;

		public String lastname;

		@Generated( event = {EventType.INSERT, EventType.UPDATE} )
		@Column(columnDefinition = "varchar(600) generated always as (firstname || ' ' || lastname) stored")
		public String fullName;

		@CurrentUser.LoggedUserMutinyInsert
		public String createdBy;

		public GeneratedRegularParent() {
		}

		public GeneratedRegularParent(String firstname, String lastname) {
			this.firstname = firstname;
			this.lastname = lastname;
		}
	}

	@Entity(name = "GeneratedRegular")
	static class GeneratedRegular extends GeneratedRegularParent {
		@Temporal(value = TemporalType.TIMESTAMP)
		@Generated( event = EventType.INSERT )
		@Column(columnDefinition = "timestamp")
		@ColumnDefault("current_timestamp")
		public Date createdAt;

		@CurrentUser.LoggedUserStageAlways
		public String updatedBy;

		public String never;

		public GeneratedRegular() {
		}

		public GeneratedRegular(String firstname, String lastname) {
			super( firstname, lastname );
		}
	}

}
