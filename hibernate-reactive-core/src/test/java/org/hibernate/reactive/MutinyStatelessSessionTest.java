/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.*;
import jakarta.persistence.criteria.*;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(value = 10, timeUnit = MINUTES)

public class MutinyStatelessSessionTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( GuineaPig.class );
	}

	@Test
	public void testStatelessSession(VertxTestContext context) {
		GuineaPig pig = new GuineaPig( "Aloi" );
		test( context, getMutinySessionFactory().withStatelessSession( ss -> ss
				.insert( pig )
				.chain( v -> ss.createSelectionQuery( "from GuineaPig where name=:n", GuineaPig.class )
						.setParameter( "n", pig.name )
						.getResultList() )
				.invoke( list -> {
					assertFalse( list.isEmpty() );
					assertEquals( 1, list.size() );
					assertThatPigsAreEqual( pig, list.get( 0 ) );
				} )
				.chain( v -> ss.get( GuineaPig.class, pig.id ) )
				.chain( p -> {
					assertThatPigsAreEqual( pig, p );
					p.name = "X";
					return ss.update( p );
				} )
				.chain( v -> ss.refresh( pig ) )
				.invoke( v -> assertEquals( pig.name, "X" ) )
				.chain( v -> ss.createMutationQuery( "update GuineaPig set name='Y'" ).executeUpdate() )
				.chain( v -> ss.refresh( pig ) )
				.invoke( v -> assertEquals( pig.name, "Y" ) )
				.chain( v -> ss.delete( pig ) )
				.chain( v -> ss.createSelectionQuery( "from GuineaPig", GuineaPig.class ).getResultList() )
				.invoke( list -> assertTrue( list.isEmpty() ) ) )
		);
	}

	@Test
	public void testStatelessSessionWithNamed(VertxTestContext context) {
		GuineaPig pig = new GuineaPig( "Aloi" );
		test( context, getMutinySessionFactory().withStatelessSession( ss -> ss
				.insert( pig )
				.chain( v -> ss.createNamedQuery( "findbyname", GuineaPig.class )
						.setParameter( "n", pig.name )
						.getResultList() )
				.invoke( list -> {
					assertFalse( list.isEmpty() );
					assertEquals( 1, list.size() );
					assertThatPigsAreEqual( pig, list.get( 0 ) );
				} )
				.chain( v -> ss.get( GuineaPig.class, pig.id ) )
				.chain( p -> {
					assertThatPigsAreEqual( pig, p );
					p.name = "X";
					return ss.update( p );
				} )
				.chain( v -> ss.refresh( pig ) )
				.invoke( v -> assertEquals( pig.name, "X" ) )
				.chain( v -> ss.createNamedQuery( "updatebyname" ).executeUpdate() )
				.chain( v -> ss.refresh( pig ) )
				.invoke( v -> assertEquals( pig.name, "Y" ) )
				.chain( v -> ss.delete( pig ) )
				.chain( v -> ss.createNamedQuery( "findall" ).getResultList() )
				.invoke( list -> assertTrue( list.isEmpty() ) ) )
		);
	}

	@Test
	public void testStatelessSessionWithNative(VertxTestContext context) {
		GuineaPig pig = new GuineaPig( "Aloi" );
		test( context, getMutinySessionFactory().openStatelessSession()
				.chain( ss -> ss.insert( pig )
						.chain( v -> ss
								.createNativeQuery( "select * from Piggy where name=:n", GuineaPig.class )
								.setParameter( "n", pig.name )
								.getResultList() )
						.invoke( list -> {
							assertFalse( list.isEmpty() );
							assertEquals( 1, list.size() );
							assertThatPigsAreEqual( pig, list.get( 0 ) );
						} )
						.chain( v -> ss.get( GuineaPig.class, pig.id ) )
						.chain( p -> {
							assertThatPigsAreEqual( pig, p );
							p.name = "X";
							return ss.update( p );
						} )
						.chain( v -> ss.refresh( pig ) )
						.invoke( v -> assertEquals( pig.name, "X" ) )
						.chain( v -> ss.createNativeQuery( "update Piggy set name='Y'" )
								.executeUpdate() )
						.invoke( rows -> assertEquals( 1, rows ) )
						.chain( v -> ss.refresh( pig ) )
						.invoke( v -> assertEquals( pig.name, "Y" ) )
						.chain( v -> ss.delete( pig ) )
						.chain( v -> ss.createNativeQuery( "select id from Piggy" ).getResultList() )
						.invoke( list -> assertTrue( list.isEmpty() ) )
						.chain( v -> ss.close() ) )
		);
	}

	@Test
	public void testStatelessSessionGetMultiple(VertxTestContext context) {
		GuineaPig a = new GuineaPig("A");
		GuineaPig b = new GuineaPig("B");
		GuineaPig c = new GuineaPig("C");
		test( context, getMutinySessionFactory().openStatelessSession()
				.chain( ss -> ss.insertMultiple( List.of(a, b, c) )
						.chain( v -> ss.get( GuineaPig.class, a.id, c.id ) )
						.invoke( list -> {
							assertEquals( 2, list.size() );
							assertThatPigsAreEqual( a, list.get( 0 ) );
							assertThatPigsAreEqual( c, list.get( 1 ) );
						})
						.chain( v -> ss.close() ) )
		);
	}

	@Test
	public void testStatelessSessionCriteria(VertxTestContext context) {
		GuineaPig pig = new GuineaPig( "Aloi" );
		GuineaPig mate = new GuineaPig("Aloina");
		pig.mate = mate;

		CriteriaBuilder cb = getSessionFactory().getCriteriaBuilder();

		CriteriaQuery<GuineaPig> query = cb.createQuery( GuineaPig.class );
		Root<GuineaPig> gp = query.from( GuineaPig.class );
		query.where( cb.equal( gp.get( "name" ), cb.parameter( String.class, "n" ) ) );
		query.orderBy( cb.asc( gp.get( "name" ) ) );

		CriteriaUpdate<GuineaPig> update = cb.createCriteriaUpdate( GuineaPig.class );
		Root<GuineaPig> updatedPig = update.from(GuineaPig.class);
		update.set( "name", "Bob" );
		update.where( updatedPig.get( "mate" ).isNotNull() );

		CriteriaDelete<GuineaPig> delete = cb.createCriteriaDelete( GuineaPig.class );
		Root<GuineaPig> deletedPig = delete.from( GuineaPig.class );
		delete.where( deletedPig.get( "mate" ).isNotNull() );

		test( context, getMutinySessionFactory().openStatelessSession()
				.chain( ss -> ss.insertMultiple( List.of(mate, pig) )
						.chain( v -> ss.createQuery( query )
								.setParameter( "n", pig.name )
								.getResultList() )
						.invoke( list -> {
							assertFalse( list.isEmpty() );
							assertEquals( 1, list.size() );
							assertThatPigsAreEqual( pig, list.get( 0 ) );
						} )
						.chain( v -> ss.createQuery( update ).executeUpdate() )
						.invoke( rows -> assertEquals( 1, rows ) )
						.chain( v -> ss.createQuery( delete ).executeUpdate() )
						.invoke( rows -> assertEquals( 1, rows ) )
						.chain( v -> ss.close() ) )
		);
	}

	@Test
	public void testTransactionPropagation(VertxTestContext context) {
		test( context, getMutinySessionFactory().withStatelessSession(
				session -> session.withTransaction( transaction -> session.createSelectionQuery( "from GuineaPig", GuineaPig.class )
						.getResultList()
						.chain( list -> {
							assertNotNull( session.currentTransaction() );
							assertFalse( session.currentTransaction().isMarkedForRollback() );
							session.currentTransaction().markForRollback();
							assertTrue( session.currentTransaction().isMarkedForRollback() );
							assertTrue( transaction.isMarkedForRollback() );
							return session.withTransaction( t -> {
								assertTrue( t.isMarkedForRollback() );
								return session.createSelectionQuery( "from GuineaPig", GuineaPig.class ).getResultList();
							} );
						} ) )
		) );
	}

	@Test
	public void testSessionPropagation(VertxTestContext context) {
		test( context, getMutinySessionFactory().withStatelessSession(
				session -> session.createSelectionQuery( "from GuineaPig", GuineaPig.class ).getResultList()
						.chain( list -> getMutinySessionFactory().withStatelessSession( s -> {
							assertEquals( session, s );
							return s.createSelectionQuery( "from GuineaPig", GuineaPig.class ).getResultList();
						} ) )
		) );
	}

	private void assertThatPigsAreEqual( GuineaPig expected, GuineaPig actual) {
		assertNotNull( actual );
		assertEquals( expected.getId(), actual.getId() );
		assertEquals( expected.getName(), actual.getName() );
	}

	@NamedQuery(name = "findbyname", query = "from GuineaPig where name=:n")
	@NamedQuery(name = "updatebyname", query = "update GuineaPig set name='Y'")
	@NamedQuery(name = "findall", query = "from GuineaPig")

	@Entity(name = "GuineaPig")
	@Table(name = "Piggy")
	public static class GuineaPig {
		@Id
		@GeneratedValue
		private Integer id;
		private String name;
		@Version
		private int version;

		@ManyToOne
		private GuineaPig mate;

		public GuineaPig() {
		}

		public GuineaPig(String name) {
			this.name = name;
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

		@Override
		public String toString() {
			return id + ": " + name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			GuineaPig guineaPig = (GuineaPig) o;
			return Objects.equals( name, guineaPig.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}
	}
}
