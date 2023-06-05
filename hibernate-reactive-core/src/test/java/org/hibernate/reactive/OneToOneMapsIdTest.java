/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.MapsId;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;

public class OneToOneMapsIdTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( PersonDetails.class, Person.class );
	}

	@Test
	public void verifyParentIdIsSet(VertxTestContext context) {
		Person person = new Person( "Joshua", 1 );
		PersonDetails personDetails = new PersonDetails( "Josh", person );

		test( context, getSessionFactory()
				.withTransaction( s -> s.persist( person, personDetails ) )
				.thenCompose( v -> getSessionFactory()
						.withSession( s -> s.find( PersonDetails.class, 1 ) ) )
				.thenAccept( Assertions::assertNotNull )
		);
	}

	@Entity(name = "Person")
	@Table(name = "MapsIdPerson")
	public static class Person  {
		@Id
		private Integer id;

		private String name;

		public Person() {}

		public Person(String name, Integer id) {
			this.name = name;
			this.id = id;
		}
	}

	@Entity(name = "PersonDetails")
	@Table(name = "MapsIdPersonDetails")
	public static class PersonDetails  {
		@Id
		private Integer id;

		private String nickName;

		@OneToOne
		@MapsId
		private Person person;

		public PersonDetails(String nickName, Person person) {
			this.nickName = nickName;
			this.person = person;
		}

		public PersonDetails() {}

		public Person getPerson() {
			return person;
		}
	}
}
