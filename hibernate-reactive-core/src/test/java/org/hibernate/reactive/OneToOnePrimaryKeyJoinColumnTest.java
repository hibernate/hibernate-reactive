/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToOne;
import javax.persistence.PrimaryKeyJoinColumn;
import javax.persistence.Table;


import org.junit.Test;

import io.vertx.ext.unit.TestContext;

public class OneToOnePrimaryKeyJoinColumnTest  extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( PersonDetails.class, Person.class );
	}

	@Test
	public void verifyParentKeyIsSet(TestContext context) {
		Person person = new Person( "Joshua", 1 );
		PersonDetails personDetails = new PersonDetails( "Josh", person );

		test( context, openSession()
				.thenCompose( session -> session
						.persist( person, personDetails )
						.thenCompose( v -> session.flush() ) )
				.thenCompose( v -> openSession() )
				.thenAccept( session -> session
						.find( OneToOneMapsIdTest.PersonDetails.class, 1 )
						.thenAccept( context::assertNotNull ) )
		);
	}

	@Entity(name = "Person")
	@Table(name = "PrimaryKeyPerson")
	public static class Person  {
		@Id
		private Integer id;

		private String name;

		public Person() {}

		public Person(String name, Integer id) {
			this.name = name;
			this.id = id;
		}

		public Integer getId() {
			return id;
		}
	}

	@Entity(name = "PersonDetails")
	@Table(name = "PrimaryKeyPersonDetails")
	public static class PersonDetails  {
		@Id
		private Integer id;

		private String nickName;

		@OneToOne
		@PrimaryKeyJoinColumn
		private Person person;

		public PersonDetails(String nickName, Person person) {
			this.nickName = nickName;
			this.person = person;
			this.id = person.getId();
		}

		public PersonDetails() {}

		public Person getPerson() {
			return person;
		}
	}
}
