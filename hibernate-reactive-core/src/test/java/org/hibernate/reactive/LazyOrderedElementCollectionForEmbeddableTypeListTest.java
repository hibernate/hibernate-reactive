/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.Hibernate;
import org.hibernate.cfg.Configuration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Embeddable;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OrderBy;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Tests @{@link ElementCollection} on a {@link java.util.Set} of basic types.
 * <p>
 * Example:
 * {@code
 *     class Person {
 *         @ElementCollection
 *         List<Phone> phones;
 *     }
 *
 *
 *     @Embeddable
 *     public static class Phone {
 *     ...
 *     }
 * }
 * </p>,
 *
 * @see EagerElementCollectionForBasicTypeListTest
 * @see EagerElementCollectionForBasicTypeSetTest
 */
public class LazyOrderedElementCollectionForEmbeddableTypeListTest extends BaseReactiveTest {

	private Person thePerson;

	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Person.class );
		return configuration;
	}

	@After
	public void cleanDb(TestContext context) {
		test( context, deleteEntities( "Person" ) );
	}

	@Before
	public void populateDb(TestContext context) {
		List<Phone> phones = new ArrayList<>();
		phones.add( new Phone( "999-999-9999" ) );
		phones.add( new Phone( "111-111-1111" ) );
		thePerson = new Person( 777777, "Claude", phones );

		test( context, getSessionFactory().withTransaction( (s, t) -> s.persist( thePerson ) ) );
	}

	@Test
	public void persistWithMutinyAPI(TestContext context) {
		List<Phone> phones = new ArrayList<>();
		phones.add( new Phone( "888" ) );
		phones.add( new Phone( "555" ) );
		Person johnny = new Person( 999, "Johnny English", phones );

		test (
				context,
				getMutinySessionFactory()
						.withTransaction( (session, tx) -> session.persist( johnny ) )
						.chain( () -> getMutinySessionFactory()
								.withTransaction( (session, tx) -> session.find( Person.class, johnny.getId() )
										.invoke( result -> context.assertFalse( Hibernate.isInitialized( result.phones ) ) )
										.chain ( result -> session.fetch(result.phones) )
										.invoke( found -> assertPhones( context, found, "555", "888" ) )
								)
						)
		);
	}

	@Test
	public void joinFetch(TestContext context) {
		test (
				context,
				getMutinySessionFactory()
						.withTransaction( (session, tx) -> session.createQuery("from Person p join fetch p.phones", Person.class)
								.getSingleResult().invoke( result -> {
									context.assertTrue( Hibernate.isInitialized(result.phones) );
									assertPhones( context, result.phones, "111-111-1111", "999-999-9999" );
								} )
						)
		);
	}

	private static void assertPhones(TestContext context, List<Phone> list, String... phones) {
		context.assertEquals( phones.length, list.size() );
		for (int i=0; i<phones.length; i++) {
			context.assertEquals( phones[i], list.get(i).getNumber() );
		}
	}

	@Entity(name = "Person")
	public static class Person {

		@Id
		private Integer id;

		private String name;

		@ElementCollection
		@OrderBy("country, number")
		private List<Phone> phones = new ArrayList<>();

		public Person() {
		}

		public Person(Integer id, String name, List<Phone> phones) {
			this.id = id;
			this.name = name;
			this.phones = phones;
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

		public Phone getPhone(String number) {
			for( Phone phone : getPhones() ) {
				if( phone.getNumber().equals( number ) ) {
					return phone;
				}
			}
			return null;
		}

		public void setPhones(List<Phone> phones) {
			this.phones  = phones;
		}

		public List<Phone> getPhones() {
			return phones;
		}
	}

	@Embeddable
	public static class Phone {

		@Column(name = "`number`")
		private String number;

		private String country;

		public Phone() {
		}

		public Phone(String number) {
			this( "UK", number );
		}

		public Phone(String country, String number) {
			this.country = country;
			this.number = number;
		}

		public String getCountry() {
			return country;
		}

		public void setCountry(String country) {
			this.country = country;
		}

		public String getNumber() {
			return number;
		}

		public void setNumber(String number) {
			this.number = number;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Phone phone = (Phone) o;
			return Objects.equals( number, phone.number );
		}

		@Override
		public int hashCode() {
			return Objects.hash( number );
		}
	}
}
