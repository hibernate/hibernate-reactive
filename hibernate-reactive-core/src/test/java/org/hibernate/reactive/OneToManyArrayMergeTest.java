/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 2, timeUnit = MINUTES)
public class OneToManyArrayMergeTest extends BaseReactiveTest {

	private final static Long USER_ID = 1L;
	private final static Long ADMIN_ROLE_ID = 2L;
	private final static Long USER_ROLE_ID = 3L;
	private final static String UPDATED_FIRSTNAME = "UPDATED FIRSTNAME";
	private final static String UPDATED_LASTNAME = "UPDATED LASTNAME";

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( User.class, Role.class );
	}

	@BeforeEach
	public void populateDb(VertxTestContext context) {
		Role adminRole = new Role( ADMIN_ROLE_ID, "admin" );
		Role userRole = new Role( USER_ROLE_ID, "user" );
		User user = new User( USER_ID, "first", "last", adminRole );
		test(
				context, getMutinySessionFactory()
						.withTransaction( s -> s.persistAll( user, adminRole, userRole ) )
		);
	}

	@Test
	public void testMerge(VertxTestContext context) {
		test(
				context, getMutinySessionFactory()
						.withTransaction( s -> s.find( User.class, USER_ID ) )
						.chain( user -> getMutinySessionFactory()
								.withTransaction( s -> s
										.createQuery( "FROM Role", Role.class )
										.getResultList() )
								.map( roles -> {
									user.addAll( roles );
									user.setFirstname( UPDATED_FIRSTNAME );
									user.setLastname( UPDATED_LASTNAME );
									return user;
								} )
						)
						.chain( user -> {
									assertThat( user.getFirstname() ).isEqualTo( UPDATED_FIRSTNAME );
									assertThat( user.getLastname() ).isEqualTo( UPDATED_LASTNAME );
									assertThat( user.getRoles() ).hasSize( 2 );
									return getMutinySessionFactory()
											.withTransaction( s -> s.merge( user ) );
								}
						)
						.chain( v -> getMutinySessionFactory()
								.withTransaction( s -> s.find( User.class, USER_ID ) )
						)
						.invoke( user -> {
									 Role adminRole = new Role( ADMIN_ROLE_ID, "admin" );
									 Role userRole = new Role( USER_ROLE_ID, "user" );
									 assertThat( user.getFirstname() ).isEqualTo( UPDATED_FIRSTNAME );
									 assertThat( user.getLastname() ).isEqualTo( UPDATED_LASTNAME );
									 assertThat( user.getRoles() ).containsExactlyInAnyOrder(
											 adminRole,
											 userRole
									 );
								 }
						)
		);
	}

	@Entity(name = "User")
	@Table(name = "USER_TABLE")
	public static class User {

		@Id
		private Long id;

		private String firstname;

		private String lastname;

		@OneToMany(fetch = FetchType.EAGER)
		private Role[] roles;

		public User() {
		}

		public User(Long id, String firstname, String lastname, Role... roles) {
			this.id = id;
			this.firstname = firstname;
			this.lastname = lastname;
			this.roles = new Role[roles.length];
			System.arraycopy( roles, 0, this.roles, 0, roles.length );
		}

		public Long getId() {
			return id;
		}

		public String getFirstname() {
			return firstname;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		public String getLastname() {
			return lastname;
		}

		public void setLastname(String lastname) {
			this.lastname = lastname;
		}

		public Role[] getRoles() {
			return roles;
		}

		public void addAll(List<Role> roles) {
			this.roles = new Role[roles.size()];
			for ( int i = 0; i < roles.size(); i++ ) {
				this.roles[i] = roles.get( i );
			}
		}
	}

	@Entity(name = "Role")
	@Table(name = "ROLE_TABLE")
	public static class Role {

		@Id
		private Long id;
		private String code;

		public Role() {
		}

		public Role(Long id, String code) {
			this.id = id;
			this.code = code;
		}

		public Object getId() {
			return id;
		}

		@Override
		public boolean equals(Object o) {
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Role role = (Role) o;
			return Objects.equals( id, role.id ) && Objects.equals( code, role.code );
		}

		@Override
		public int hashCode() {
			return Objects.hash( id, code );
		}

		@Override
		public String toString() {
			return "Role{" + code + '}';
		}
	}
}
