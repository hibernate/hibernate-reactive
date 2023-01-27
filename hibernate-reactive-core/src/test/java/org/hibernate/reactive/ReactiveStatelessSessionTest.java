/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.junit.Test;

import io.vertx.ext.unit.TestContext;
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


public class ReactiveStatelessSessionTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( GuineaPig.class );
	}

	@Test
	public void testStatelessSession(TestContext context) {
		GuineaPig pig = new GuineaPig( "Aloi" );
		test( context, getSessionFactory().withStatelessSession( ss -> ss
				.insert( pig )
				.thenCompose( v -> ss.createQuery( "from GuineaPig where name=:n", GuineaPig.class )
						.setParameter( "n", pig.name )
						.getResultList() )
				.thenAccept( list -> {
					context.assertFalse( list.isEmpty() );
					context.assertEquals( 1, list.size() );
					assertThatPigsAreEqual( context, pig, list.get( 0 ) );
				} )
				.thenCompose( v -> ss.get( GuineaPig.class, pig.id ) )
				.thenCompose( p -> {
					assertThatPigsAreEqual( context, pig, p );
					p.name = "X";
					return ss.update( p );
				} )
				.thenCompose( v -> ss.refresh( pig ) )
				.thenAccept( v -> context.assertEquals( pig.name, "X" ) )
				.thenCompose( v -> ss.createQuery( "update GuineaPig set name='Y'" ).executeUpdate() )
				.thenCompose( v -> ss.refresh( pig ) )
				.thenAccept( v -> context.assertEquals( pig.name, "Y" ) )
				.thenCompose( v -> ss.delete( pig ) )
				.thenCompose( v -> ss.createQuery( "from GuineaPig" ).getResultList() )
				.thenAccept( list -> context.assertTrue( list.isEmpty() ) ) )
		);
	}

	@Test
	public void testStatelessSessionWithNamed(TestContext context) {
		GuineaPig pig = new GuineaPig( "Aloi" );
		test( context, getSessionFactory().withStatelessSession( ss -> ss
				.insert( pig )
				.thenCompose( v -> ss.createNamedQuery( "findbyname", GuineaPig.class )
						.setParameter( "n", pig.name )
						.getResultList() )
				.thenAccept( list -> {
					context.assertFalse( list.isEmpty() );
					context.assertEquals( 1, list.size() );
					assertThatPigsAreEqual( context, pig, list.get( 0 ) );
				} )
				.thenCompose( v -> ss.get( GuineaPig.class, pig.id ) )
				.thenCompose( p -> {
					assertThatPigsAreEqual( context, pig, p );
					p.name = "X";
					return ss.update( p );
				} )
				.thenCompose( v -> ss.refresh( pig ) )
				.thenAccept( v -> context.assertEquals( pig.name, "X" ) )
				.thenCompose( v -> ss.createNamedQuery( "updatebyname" ).executeUpdate() )
				.thenCompose( v -> ss.refresh( pig ) )
				.thenAccept( v -> context.assertEquals( pig.name, "Y" ) )
				.thenCompose( v -> ss.delete( pig ) )
				.thenCompose( v -> ss.createNamedQuery( "findall" ).getResultList() )
				.thenAccept( list -> context.assertTrue( list.isEmpty() ) ) )
		);
	}

	@Test
	public void testStatelessSessionWithNative(TestContext context) {
		GuineaPig pig = new GuineaPig( "Aloi" );
		test( context, getSessionFactory().openStatelessSession()
				.thenCompose( ss -> ss.insert( pig )
						.thenCompose( v -> ss
								.createNativeQuery( "select * from Piggy where name=:n", GuineaPig.class )
								.setParameter( "n", pig.name )
								.getResultList() )
						.thenAccept( list -> {
							context.assertFalse( list.isEmpty() );
							context.assertEquals( 1, list.size() );
							assertThatPigsAreEqual( context, pig, list.get( 0 ) );
						} )
						.thenCompose( v -> ss.get( GuineaPig.class, pig.id ) )
						.thenCompose( p -> {
							assertThatPigsAreEqual( context, pig, p );
							p.name = "X";
							return ss.update( p );
						} )
						.thenCompose( v -> ss.refresh( pig ) )
						.thenAccept( v -> context.assertEquals( pig.name, "X" ) )
						.thenCompose( v -> ss
								.createNativeQuery( "update Piggy set name='Y'" )
								.executeUpdate() )
						.thenAccept( rows -> context.assertEquals( 1, rows ) )
						.thenCompose( v -> ss.refresh( pig ) )
						.thenAccept( v -> context.assertEquals( pig.name, "Y" ) )
						.thenCompose( v -> ss.delete( pig ) )
						.thenCompose( v -> ss.createNativeQuery( "select id from Piggy" ).getResultList() )
						.thenAccept( list -> context.assertTrue( list.isEmpty() ) )
						.thenCompose( v -> ss.close() ) )
		);
	}

	@Test
	public void testStatelessSessionCriteria(TestContext context) {
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
							context.assertFalse( list.isEmpty() );
							context.assertEquals( 1, list.size() );
							assertThatPigsAreEqual( context, pig, list.get( 0 ) );
						} )
						.thenCompose( v -> ss.createQuery( update ).executeUpdate() )
						.thenAccept( rows -> context.assertEquals( 1, rows ) )
						.thenCompose( v -> ss.createQuery( delete ).executeUpdate() )
						.thenAccept( rows -> context.assertEquals( 1, rows ) )
						.thenCompose( v -> ss.close() ) )
		);
	}

	@Test
	public void testTransactionPropagation(TestContext context) {
		test( context, getSessionFactory().withStatelessSession(
				session -> session.withTransaction( transaction -> session.createQuery( "from GuineaPig" )
						.getResultList()
						.thenCompose( list -> {
							context.assertNotNull( session.currentTransaction() );
							context.assertFalse( session.currentTransaction().isMarkedForRollback() );
							session.currentTransaction().markForRollback();
							context.assertTrue( session.currentTransaction().isMarkedForRollback() );
							context.assertTrue( transaction.isMarkedForRollback() );
							return session.withTransaction( t -> {
								context.assertTrue( t.isMarkedForRollback() );
								return session.createQuery( "from GuineaPig" ).getResultList();
							} );
						} ) )
		) );
	}

	@Test
	public void testSessionPropagation(TestContext context) {
		test( context, getSessionFactory().withStatelessSession(
				session -> session.createQuery( "from GuineaPig" ).getResultList()
						.thenCompose( list -> getSessionFactory().withStatelessSession( s -> {
							context.assertEquals( session, s );
							return s.createQuery( "from GuineaPig" ).getResultList();
						} ) )
		) );
	}

	private void assertThatPigsAreEqual(TestContext context, GuineaPig expected, GuineaPig actual) {
		context.assertNotNull( actual );
		context.assertEquals( expected.getId(), actual.getId() );
		context.assertEquals( expected.getName(), actual.getName() );
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
