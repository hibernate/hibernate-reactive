/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
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
				.thenCompose( session -> session.find( PersonDetails.class, 1 ) )
				.thenAccept( details -> context.assertEquals( personDetails, details ) )
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

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( !( o instanceof Person ) ) {
				return false;
			}
			Person person = (Person) o;
			return name.equals( person.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
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

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( !( o instanceof PersonDetails ) ) {
				return false;
			}
			PersonDetails that = (PersonDetails) o;
			return nickName.equals( that.nickName ) && Objects.equals( person, that.person );
		}

		@Override
		public int hashCode() {
			return Objects.hash( nickName );
		}
	}
}
