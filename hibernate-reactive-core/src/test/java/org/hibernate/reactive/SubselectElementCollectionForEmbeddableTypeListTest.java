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
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

	Person p1;
	Person p2;
	Person p3;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Person.class );
	}

	public Uni<?> populateDbMutiny() {
		if( p1 == null ) {
			List<Phone> phones1 = new ArrayList<>();
			phones1.add( new Phone( "999-999-9999" ) );
			phones1.add( new Phone( "111-111-1111" ) );
			p1 = new Person( 777777, "Claude", phones1 );
			List<Phone> phones2 = new ArrayList<>();
			phones2.add( new Phone( "999-999-9999" ) );
			phones2.add( new Phone( "111-111-1111" ) );
			p2 = new Person( 888888, "Solene", phones2 );
			List<Phone> phones3 = new ArrayList<>();
			phones3.add( new Phone( "999-999-9999" ) );
			phones3.add( new Phone( "111-111-1111" ) );
			p3 = new Person( 999999, "Claude", phones3 );
		}

		return getMutinySessionFactory()
				.withTransaction( session -> session.persistAll( p1, p2, p3 ).call(session::flush) );
	}

	@Test
	public void persistWithMutinyAPI(VertxTestContext context) {
		List<Phone> phones = new ArrayList<>();
		phones.add( new Phone( "888" ) );
		phones.add( new Phone( "555" ) );
		Person johnny = new Person( 999, "Johnny English", phones );

		test (
				context,
				populateDbMutiny()
						.call( () -> getMutinySessionFactory()
						.withTransaction( session -> session.persist( johnny ) ) )
						.chain( () -> getMutinySessionFactory()
								.withTransaction( session -> session.find( Person.class, johnny.getId() )
										.invoke( result -> assertFalse( Hibernate.isInitialized( result.phones ) ) )
										.chain ( result -> session.fetch(result.phones) )
										.invoke( found -> assertPhones( context, found, "888", "555" ) )
								)
						)
						.chain( () -> getMutinySessionFactory()
								.withTransaction( session -> session
										.find( Person.class, johnny.getId() )
										.chain( session::remove )
								)
						)
		);
	}

	@Test
	public void joinFetch(VertxTestContext context) {
		test (
				context,
				populateDbMutiny()
						.call( () -> getMutinySessionFactory()
						.withTransaction( session -> session.createQuery("select distinct p from Person p join fetch p.phones where p.name='Claude'", Person.class)
								.getResultList()
								.invoke( all -> assertEquals( 2, all.size() ) )
								.invoke( all -> all.forEach( result -> {
									assertTrue( Hibernate.isInitialized(result.phones) );
									assertPhones( context, result.phones, "111-111-1111", "999-999-9999" );
								} ) )
						) )
		);
	}

	@Test
	public void noJoinFetch(VertxTestContext context) {
		test (
				context,
				populateDbMutiny()
						.call( () -> getMutinySessionFactory()
						.withTransaction( session -> session.createQuery("from Person p where p.name='Claude'", Person.class)
								.getResultList()
								.invoke( all -> assertEquals( 2, all.size() ) )
								.invoke( all -> all.forEach( result -> assertFalse( Hibernate.isInitialized( result.phones ) ) ) )
								.chain ( all -> session.fetch( all.get(0).phones ) )
								.invoke( phones -> {
									assertTrue( Hibernate.isInitialized(phones) );
									assertPhones( context, phones, "111-111-1111", "999-999-9999" );
								} )
						) )
		);
	}

	private static void assertPhones(VertxTestContext context, List<Phone> list, String... phones) {
		assertEquals( phones.length, list.size() );
		for ( String phone : phones ) {
			assertTrue( phonesContainNumber( list, phone ) );
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
