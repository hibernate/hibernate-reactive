/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.ManyToOne;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 1, timeUnit = MINUTES)
public class ManyToOneIdClassTest extends BaseReactiveTest {

	private final static String USER_NAME = "user";
	private final static String SUBSYSTEM_ID = "1";

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( SystemUser.class, Subsystem.class );
	}

	@BeforeEach
	public void populateDb(VertxTestContext context) {
		Subsystem subsystem = new Subsystem( SUBSYSTEM_ID, "sub 1" );
		SystemUser systemUser = new SystemUser( subsystem, USER_NAME, "system 1" );
		test(
				context, getMutinySessionFactory()
						.withTransaction( (s, t) -> s.persistAll( subsystem, systemUser ) )
		);
	}

	@AfterEach
	public void after(VertxTestContext context) {
		test( context, cleanDb() );
	}

	@Test
	public void testQuery(VertxTestContext context) {
		test(
				context, openSession()
						.thenAccept( session -> session.createQuery( "SELECT s FROM SystemUser s", SystemUser.class )
								.getResultList().thenAccept( list -> {
									assertThat( list.size() ).isEqualTo( 1 );
									SystemUser systemUser = list.get( 0 );
									assertThat( systemUser.getSubsystem().getId() ).isEqualTo( SUBSYSTEM_ID );
									assertThat( systemUser.getUsername() ).isEqualTo( USER_NAME );
								} ) )
		);
	}

	@Entity(name = "SystemUser")
	@IdClass(PK.class)
	public static class SystemUser {

		@Id
		@ManyToOne(fetch = FetchType.LAZY)
		private Subsystem subsystem;

		@Id
		private String username;

		private String name;

		public SystemUser() {
		}

		public SystemUser(Subsystem subsystem, String username, String name) {
			this.subsystem = subsystem;
			this.username = username;
			this.name = name;
		}

		public Subsystem getSubsystem() {
			return subsystem;
		}

		public String getUsername() {
			return username;
		}

		public String getName() {
			return name;
		}
	}

	@Entity(name = "Subsystem")
	public static class Subsystem {

		@Id
		private String id;

		private String description;

		public Subsystem() {
		}

		public Subsystem(String id, String description) {
			this.id = id;
			this.description = description;
		}

		public String getId() {
			return id;
		}

		public String getDescription() {
			return description;
		}
	}

	public static class PK {

		private Subsystem subsystem;

		private String username;

		public PK(Subsystem subsystem, String username) {
			this.subsystem = subsystem;
			this.username = username;
		}

		private PK() {
		}
	}

}
