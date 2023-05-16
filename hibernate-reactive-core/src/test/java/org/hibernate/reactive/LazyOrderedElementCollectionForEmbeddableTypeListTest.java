/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.hibernate.Hibernate;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.OrderBy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests @{@link ElementCollection} on a {@link java.util.Set} of basic types.
 * <p>
 * Example:
 * {@code
 * class Person {
 *
 * @ElementCollection List<Phone> phones;
 * }
 * @Embeddable public static class Phone {
 * ...
 * }
 * }
 * </p>,
 * @see EagerElementCollectionForBasicTypeListTest
 * @see EagerElementCollectionForBasicTypeSetTest
 */
public class LazyOrderedElementCollectionForEmbeddableTypeListTest extends BaseReactiveTest {

	private Person thePerson;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Person.class );
	}

	public Uni<?> populateDbMutiny() {
		if( thePerson == null ) {
			List<Phone> phones = new ArrayList<>();
			phones.add( new Phone( "999-999-9999" ) );
			phones.add( new Phone( "111-111-1111" ) );
			thePerson = new Person( 777777, "Claude", phones );
		}

		return getMutinySessionFactory().withTransaction( (s, t) -> s.persist( thePerson ) );
	}

	@Test
	public void persistWithMutinyAPI(VertxTestContext context) {
		List<Phone> phones = new ArrayList<>();
		phones.add( new Phone( "888" ) );
		phones.add( new Phone( "555" ) );
		Person johnny = new Person( 999, "Johnny English", phones );

		test(
				context,
				populateDbMutiny()
						.call( () -> getMutinySessionFactory()
						.withTransaction( (session, tx) -> session.persist( johnny ) ) )
						.chain( () -> getMutinySessionFactory()
								.withTransaction( (session, tx) -> session.find( Person.class, johnny.getId() )
										.invoke( result -> assertFalse( Hibernate.isInitialized( result.phones ) ) )
										.chain( result -> session.fetch( result.phones ) )
										.invoke( found -> assertPhones( context, found, "555", "888" ) )
								)
						)
		);
	}

	@Test
	public void joinFetch(VertxTestContext context) {
		test(
				context,
				populateDbMutiny()
						.call( () -> getMutinySessionFactory()
						.withTransaction( (session, tx) -> session.createQuery(
														  "from Person p join fetch p.phones",
														  Person.class
												  )
												  .getSingleResult().invoke( result -> {
													  assertTrue( Hibernate.isInitialized( result.phones ) );
													  assertPhones( context, result.phones, "111-111-1111", "999-999-9999" );
												  } )
						) )
		);
	}

	private static void assertPhones(VertxTestContext context, List<Phone> list, String... phones) {
		assertEquals( phones.length, list.size() );
		for ( int i = 0; i < phones.length; i++ ) {
			assertEquals( phones[i], list.get( i ).getNumber() );
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
			for ( Phone phone : getPhones() ) {
				if ( phone.getNumber().equals( number ) ) {
					return phone;
				}
			}
			return null;
		}

		public void setPhones(List<Phone> phones) {
			this.phones = phones;
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
