/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Embeddable;
import javax.persistence.Entity;
import javax.persistence.Id;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;

public class EagerEmbeddedElementCollectionTest  extends BaseReactiveTest {

	private Person thePerson;

	@Rule
	public DatabaseSelectionRule rule = DatabaseSelectionRule.runOnlyFor( POSTGRESQL );

	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Person.class );

		return configuration;
	}

	@Before
	public void populateDb(TestContext context) {
		Phone[] phoneArray = new Phone[]{ new Phone("999-999-9999"), new Phone("111-111-1111")};
		thePerson = new Person( 777777, "Claude", phoneArray);

		Stage.Session session = openSession();

		test( context,
			  session.persist( thePerson )
					  .thenCompose( v -> session.flush() )
					  .thenCompose( v -> openSession().find( Person.class, thePerson.getId()) )
		);
	}

	/*
	 * This test performs a simple check of Person entity collection values after persisting
	 */
	@Test
	public void findPhonesElementCollectionStageSession(TestContext context) {
		Stage.Session session = openSession();

		test (
				context,
				session.find( Person.class, thePerson.getId())
						.thenAccept( found -> {
							context.assertNotNull( found );
							context.assertEquals( 2, found.getPhones().size() );
						} )
		);
	}

	@Entity(name = "Person")
	public static class Person {

		@Id
		private Integer id;

		private String name;

		@ElementCollection
		private List<Phone> phones = new ArrayList<>();

		public Person() {
		}

		public Person(Integer id, String name, Phone[] phones) {
			this.id = id;
			this.name = name;
			if( phones != null ) {
				this.phones.addAll( Arrays.asList( phones ) );
			}
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public List<Phone> getPhones() {
			return phones;
		}
	}

	@Embeddable
	public static class Phone {

		@Column(name = "`number`")
		private String number;

		public Phone() {
		}

		public Phone(String number) {
			this.number = number;
		}

		public String getNumber() {
			return number;
		}

		public void setNumber(String number) {
			this.number = number;
		}
	}
}
