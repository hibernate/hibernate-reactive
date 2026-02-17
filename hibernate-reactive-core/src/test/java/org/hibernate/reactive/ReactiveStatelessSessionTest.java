/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.NamedQuery;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaDelete;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.CriteriaUpdate;
import jakarta.persistence.criteria.Root;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 10, timeUnit = MINUTES)

public class ReactiveStatelessSessionTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( GuineaPig.class );
	}

	@Test
	public void testStatelessSession(VertxTestContext context) {
		GuineaPig pig = new GuineaPig( "Aloi" );
		test( context, getSessionFactory().withStatelessSession( ss -> ss
				.insert( pig )
				.thenCompose( v -> ss.createSelectionQuery( "from GuineaPig where name=:n", GuineaPig.class )
						.setParameter( "n", pig.name )
						.getResultList() )
				.thenAccept( list -> {
					assertThat( list ).hasSize( 1 );
					assertThatPigsAreEqual( pig, list.get( 0 ) );
				} )
				.thenCompose( v -> ss.get( GuineaPig.class, pig.id ) )
				.thenCompose( p -> {
					assertThatPigsAreEqual( pig, p );
					p.name = "X";
					return ss.update( p );
				} )
				.thenCompose( v -> ss.refresh( pig ) )
				.thenAccept( v -> assertThat( pig.name ).isEqualTo( "X" ) )
				.thenCompose( v -> ss.createMutationQuery( "update GuineaPig set name='Y'" ).executeUpdate() )
				.thenCompose( v -> ss.refresh( pig ) )
				.thenAccept( v -> assertThat( pig.name ).isEqualTo( "Y" ) )
				.thenCompose( v -> ss.delete( pig ) )
				.thenCompose( v -> ss.createSelectionQuery( "from GuineaPig", GuineaPig.class ).getResultList() )
				.thenAccept( list -> assertThat( list ).isEmpty() ) )
		);
	}

	@Test
	public void testStatelessSessionWithNamed(VertxTestContext context) {
		GuineaPig pig = new GuineaPig( "Aloi" );
		test( context, getSessionFactory().withStatelessSession( ss -> ss
				.insert( pig )
				.thenCompose( v -> ss.createNamedQuery( "findbyname", GuineaPig.class )
						.setParameter( "n", pig.name )
						.getResultList() )
				.thenAccept( list -> {
					assertThat( list ).hasSize( 1 );
					assertThatPigsAreEqual( pig, list.get( 0 ) );
				} )
				.thenCompose( v -> ss.get( GuineaPig.class, pig.id ) )
				.thenCompose( p -> {
					assertThatPigsAreEqual( pig, p );
					p.name = "X";
					return ss.update( p );
				} )
				.thenCompose( v -> ss.refresh( pig ) )
				.thenAccept( v -> assertThat( pig.name ).isEqualTo( "X" ) )
				.thenCompose( v -> ss.createNamedQuery( "updatebyname" ).executeUpdate() )
				.thenCompose( v -> ss.refresh( pig ) )
				.thenAccept( v -> assertThat( pig.name ).isEqualTo( "Y" ) )
				.thenCompose( v -> ss.delete( pig ) )
				.thenCompose( v -> ss.createNamedQuery( "findall" ).getResultList() )
				.thenAccept( list -> assertThat( list ).isEmpty() ) )
		);
	}

	@Test
	public void testStatelessSessionWithNative(VertxTestContext context) {
		GuineaPig pig = new GuineaPig( "Aloi" );
		test( context, getSessionFactory().openStatelessSession()
				.thenCompose( ss -> ss.insert( pig )
						.thenCompose( v -> ss
								.createNativeQuery( "select * from Piggy where name=:n", GuineaPig.class )
								.setParameter( "n", pig.name )
								.getResultList() )
						.thenAccept( list -> {
							assertThat( list ).hasSize( 1 );
							assertThatPigsAreEqual( pig, list.get( 0 ) );
						} )
						.thenCompose( v -> ss.get( GuineaPig.class, pig.id ) )
						.thenCompose( p -> {
							assertThatPigsAreEqual( pig, p );
							p.name = "X";
							return ss.update( p );
						} )
						.thenCompose( v -> ss.refresh( pig ) )
						.thenAccept( v -> assertThat( pig.name ).isEqualTo( "X" ) )
						.thenCompose( v -> ss
								.createNativeQuery( "update Piggy set name='Y'" )
								.executeUpdate() )
						.thenAccept( rows -> assertThat( rows ).isEqualTo( 1 ) )
						.thenCompose( v -> ss.refresh( pig ) )
						.thenAccept( v -> assertThat( pig.name ).isEqualTo( "Y" ) )
						.thenCompose( v -> ss.delete( pig ) )
						.thenCompose( v -> ss.createNativeQuery( "select id from Piggy" ).getResultList() )
						.thenAccept( list -> assertThat( list ).isEmpty() )
						.thenCompose( v -> ss.close() ) )
		);
	}

	@Test
	public void testStatelessSessionGetMultiple(VertxTestContext context) {
		GuineaPig a = new GuineaPig("A");
		GuineaPig b = new GuineaPig("B");
		GuineaPig c = new GuineaPig("C");
		test( context, getSessionFactory().openStatelessSession()
				.thenCompose( ss -> ss.insertMultiple( List.of(a, b, c) )
						.thenCompose( v -> ss.get( GuineaPig.class, a.id, c.id ) )
						.thenAccept( list -> {
							assertThat( list ).hasSize( 2 );
							assertThatPigsAreEqual( a, list.get( 0 ) );
							assertThatPigsAreEqual( c, list.get( 1 ) );
						})
						.thenCompose( v -> ss.close() ) )
		);
	}

	@Test
	public void testStatelessSessionCriteria(VertxTestContext context) {
		GuineaPig pig = new GuineaPig( "Aloi" );

		CriteriaBuilder cb = getSessionFactory().getCriteriaBuilder();

		CriteriaQuery<GuineaPig> query = cb.createQuery( GuineaPig.class );
		Root<GuineaPig> gp = query.from( GuineaPig.class );
		query.where( cb.equal( gp.get( "name" ), cb.parameter( String.class, "n" ) ) );

		CriteriaUpdate<GuineaPig> update = cb.createCriteriaUpdate( GuineaPig.class );
		update.from( GuineaPig.class );
		update.set( "name", "Bob" );

		CriteriaDelete<GuineaPig> delete = cb.createCriteriaDelete( GuineaPig.class );
		delete.from( GuineaPig.class );

		test( context, getSessionFactory().openStatelessSession()
				.thenCompose( ss -> ss.insert( pig )
						.thenCompose( v -> ss.createQuery( query )
								.setParameter( "n", pig.name )
								.getResultList() )
						.thenAccept( list -> {
							assertThat( list ).hasSize( 1 );
							assertThatPigsAreEqual( pig, list.get( 0 ) );
						} )
						.thenCompose( v -> ss.createQuery( update ).executeUpdate() )
						.thenAccept( rows -> assertThat( rows ).isEqualTo( 1 ) )
						.thenCompose( v -> ss.createQuery( delete ).executeUpdate() )
						.thenAccept( rows -> assertThat( rows ).isEqualTo( 1 ) )
						.thenCompose( v -> ss.close() ) )
		);
	}

	@Test
	public void testTransactionPropagation(VertxTestContext context) {
		test( context, getSessionFactory().withStatelessSession(
				session -> session.withTransaction( transaction -> session.createSelectionQuery( "from GuineaPig", GuineaPig.class )
						.getResultList()
						.thenCompose( list -> {
							assertThat( session.currentTransaction() ).isNotNull();
							assertThat( session.currentTransaction().isMarkedForRollback() ).isFalse();
							session.currentTransaction().markForRollback();
							assertThat( session.currentTransaction().isMarkedForRollback() ).isTrue();
							assertThat( transaction.isMarkedForRollback() ).isTrue();
							return session.withTransaction( t -> {
								assertThat( t.isMarkedForRollback() ).isTrue();
								return session.createSelectionQuery( "from GuineaPig", GuineaPig.class ).getResultList();
							} );
						} ) )
		) );
	}

	@Test
	public void testSessionPropagation(VertxTestContext context) {
		test( context, getSessionFactory().withStatelessSession(
				session -> session.createSelectionQuery( "from GuineaPig", GuineaPig.class ).getResultList()
						.thenCompose( list -> getSessionFactory().withStatelessSession( s -> {
							assertThat( s ).isEqualTo( session );
							return s.createSelectionQuery( "from GuineaPig", GuineaPig.class ).getResultList();
						} ) )
		) );
	}

	private void assertThatPigsAreEqual( GuineaPig expected, GuineaPig actual) {
		assertThat( actual ).isNotNull();
		assertThat( actual.getId() ).isEqualTo( expected.getId() );
		assertThat( actual.getName() ).isEqualTo( expected.getName() );
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
