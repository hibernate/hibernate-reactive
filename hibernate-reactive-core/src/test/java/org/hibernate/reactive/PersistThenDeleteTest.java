/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;


import org.junit.Test;

import io.vertx.ext.unit.TestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

/**
 * Verifies that scheduled changes in the reactive session
 * are being auto-flushed before a mutation query on the same
 * table space is executed.
 */
public class PersistThenDeleteTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( PersistThenDeleteTest.Person.class );
	}

	@Test
	public void testPersistThenDelete(TestContext context) {
		test(context,
			 getSessionFactory().withTransaction(
							 (s, t) -> s.persist( newPerson( "foo" ), newPerson( "bar" ), newPerson( "baz" ) )
					 )
					 .thenCompose(
							 v ->
							 getSessionFactory().withTransaction(
											 (s, t) -> s.createQuery( "from Person" ).getResultList().thenAccept( l ->
											 context.assertEquals( 3, l.size() )
											 )
							 )
					 )
					 .thenCompose(
							v ->
							getSessionFactory().withTransaction(
									(s, t) ->
										s.persist( newPerson( "critical" ) )
												.thenCompose( vo -> s.createQuery( "delete from Person" )
														.executeUpdate() )
								  )
					 )
					 .thenCompose(
							 v ->
									 getSessionFactory().withTransaction(
											 (s, t) -> s.createQuery( "from Person" ).getResultList().thenAccept( l ->
																														  context.assertEquals( 0, l.size() )
											 )
									 )
					 )
		);
	}

	@Test
	public void testDeleteThenPersist(TestContext context) {
		test(context,
			 getSessionFactory().withTransaction(
							 (s, t) -> s.persist( newPerson( "foo" ), newPerson( "bar" ), newPerson( "baz" ) )
					 )
					 .thenCompose(
							 v ->
									 getSessionFactory().withTransaction(
											 (s, t) -> s.createQuery( "from Person" ).getResultList().thenAccept( l ->
																														  context.assertEquals( 3, l.size() )
											 )
									 )
					 )
					 .thenCompose(
							 v ->
									 getSessionFactory().withTransaction(
											 (s, t) ->
													 s.createQuery( "delete from Person" ).executeUpdate()
															 .thenCompose( vo -> s.persist( newPerson( "critical" ) ) )
									 )
					 )
					 .thenCompose(
							 v ->
									 getSessionFactory().withTransaction(
											 (s, t) -> s.createQuery( "from Person" ).getResultList().thenAccept( l ->
																														  context.assertEquals( 1, l.size() )
											 )
									 )
					 )
		);
	}

	private static Person newPerson(String name) {
		final Person person = new Person();
		person.name = name;
		return person;
	}

	@Entity(name="Person")
	public static class Person {

		@Id
		@GeneratedValue
		Integer id;

		String name;
	}

}
