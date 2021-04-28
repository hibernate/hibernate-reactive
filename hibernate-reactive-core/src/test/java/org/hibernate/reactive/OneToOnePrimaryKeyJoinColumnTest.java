/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToOne;
import javax.persistence.PrimaryKeyJoinColumn;
import javax.persistence.Table;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.stage.Stage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

public class OneToOnePrimaryKeyJoinColumnTest  extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Person.class );
		configuration.addAnnotatedClass( PersonDetails.class );
		return configuration;
	}

	@Before
	public void populateDb(TestContext context) {
		Person person = new Person("Joshua", 1);
		PersonDetails personDetails = new PersonDetails("Josh", person);

		Stage.Session session = openSession();

		test( context, session.persist( person, personDetails )
				.thenCompose( v -> session.flush() )
		);
	}

	@After
	public void cleanDb(TestContext context) {
		test( context, deleteEntities( "PersonDetails", "Person" ) );
	}

	@Test
	public void verifyParentKeyIsSet(TestContext context) {
		test(
				context,
				openSession().find( PersonDetails.class, 1 )
						.thenAccept( foundPersonDetails ->
								context.assertNotNull( foundPersonDetails ) )
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
