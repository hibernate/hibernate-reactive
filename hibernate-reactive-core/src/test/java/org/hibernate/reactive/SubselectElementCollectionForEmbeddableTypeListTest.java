/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.Hibernate;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.hibernate.cfg.Configuration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Embeddable;
import javax.persistence.Entity;
import javax.persistence.Id;
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
public class SubselectElementCollectionForEmbeddableTypeListTest extends BaseReactiveTest {

	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Person.class );
		return configuration;
	}

	@Before
	public void populateDb(TestContext context) {
		List<Phone> phones1 = new ArrayList<>();
		phones1.add( new Phone( "999-999-9999" ) );
		phones1.add( new Phone( "111-111-1111" ) );
		Person p1 = new Person( 777777, "Claude", phones1 );
		List<Phone> phones2 = new ArrayList<>();
		phones2.add( new Phone( "999-999-9999" ) );
		phones2.add( new Phone( "111-111-1111" ) );
		Person p2 = new Person( 888888, "Solene", phones2 );
		List<Phone> phones3 = new ArrayList<>();
		phones3.add( new Phone( "999-999-9999" ) );
		phones3.add( new Phone( "111-111-1111" ) );
		Person p3 = new Person( 999999, "Claude", phones3 );

		test( context, openSession()
				.thenCompose( session -> session.persist( p1, p2, p3 )
						.thenCompose( v -> session.flush() ) ) );
	}

	@After
	public void cleanDb(TestContext context) {
		test( context, deleteEntities( "Person" ) );
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
										.invoke( found -> assertPhones( context, found, "888", "555" ) )
								)
						)
						.chain( () -> getMutinySessionFactory()
								.withTransaction( (session, tx) -> session.find( Person.class, johnny.getId() )
										.chain( session::remove )
								)
						)
		);
	}

	@Test
	public void joinFetch(TestContext context) {
		test (
				context,
				getMutinySessionFactory()
						.withTransaction( (session, tx) -> session.createQuery("select distinct p from Person p join fetch p.phones where p.name='Claude'", Person.class)
								.getResultList()
								.invoke( all -> context.assertEquals( 2, all.size() ) )
								.invoke( all -> all.forEach( result -> {
									context.assertTrue( Hibernate.isInitialized(result.phones) );
									assertPhones( context, result.phones, "111-111-1111", "999-999-9999" );
								} ) )
						)
		);
	}

	@Test
	public void noJoinFetch(TestContext context) {
		test (
				context,
				getMutinySessionFactory()
						.withTransaction( (session, tx) -> session.createQuery("from Person p where p.name='Claude'", Person.class)
								.getResultList()
								.invoke( all -> context.assertEquals( 2, all.size() ) )
								.invoke( all -> all.forEach( result -> context.assertFalse( Hibernate.isInitialized( result.phones ) ) ) )
								.chain ( all -> session.fetch( all.get(0).phones ) )
								.invoke( phones -> {
									context.assertTrue( Hibernate.isInitialized(phones) );
									assertPhones( context, phones, "111-111-1111", "999-999-9999" );
								} )
						)
		);
	}

	private static void assertPhones(TestContext context, List<Phone> list, String... phones) {
		context.assertEquals( phones.length, list.size() );
		for ( String phone : phones ) {
			context.assertTrue( phonesContainNumber( list, phone ) );
		}
	}

	private static boolean phonesContainNumber(List<Phone> phones, String phoneNum) {
		for ( Phone phone : phones ) {
			if ( phone.number.equals( phoneNum ) ) {
				return true;
			}
		}
		return false;
	}

	@Entity(name = "Person")
	public static class Person {

		@Id
		private Integer id;

		private String name;

		@ElementCollection
		@Fetch(FetchMode.SUBSELECT)
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
