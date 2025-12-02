/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.MapsId;
import jakarta.persistence.OneToOne;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 10, timeUnit = MINUTES)
public class OneToOneMapsIdAndGeneratedIdTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Person.class, NaturalPerson.class );
	}

	@Test
	public void testPersist(VertxTestContext context) {
		NaturalPerson naturalPerson = new NaturalPerson( "natual" );
		Person person = new Person( "person", naturalPerson );

		test(
				context, getMutinySessionFactory()
						.withTransaction( session -> session.persist( person ) )
						.chain( () -> getMutinySessionFactory()
								.withTransaction( session -> session
										.find( Person.class, person.getId() )
										.invoke( result -> {
											assertThat( result ).isNotNull();
											assertThat( result.getNaturalPerson() ).isNotNull();
											assertThat( result.getNaturalPerson().getId() ).isEqualTo( result.getId() );
										} ) )
						)
		);

	}

	@Entity
	public static class Person {

		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		private Long id;

		private String name;

		@OneToOne(mappedBy = "person", cascade = CascadeType.ALL)
		private NaturalPerson naturalPerson;

		public Person() {
		}

		public Person(String name, NaturalPerson naturalPerson) {
			this.name = name;
			this.naturalPerson = naturalPerson;
			naturalPerson.person = this;
		}

		public Long getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public NaturalPerson getNaturalPerson() {
			return naturalPerson;
		}

		@Override
		public boolean equals(Object o) {
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Person person = (Person) o;
			return Objects.equals( name, person.name );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( name );
		}
	}

	@Entity
	public static class NaturalPerson {

		@Id
		private Long id;

		@Column
		private String name;

		@OneToOne(fetch = FetchType.LAZY)
		@MapsId
		private Person person;

		public NaturalPerson() {
		}

		public NaturalPerson(String name) {
			this.name = name;
		}

		public Long getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public Person getPerson() {
			return person;
		}

		@Override
		public boolean equals(Object o) {
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			NaturalPerson that = (NaturalPerson) o;
			return Objects.equals( name, that.name );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( name );
		}
	}


}
