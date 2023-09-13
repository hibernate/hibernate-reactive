/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class UpsertTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Record.class );
	}

	@AfterEach
	public void cleanDb(VertxTestContext context) {
		test( context, getSessionFactory()
				.withTransaction( s -> s.createQuery( "delete from Record" ).executeUpdate() ) );
	}

	@Test
	public void testUpsert( VertxTestContext context) {
		Record r1 = new Record(123L,"hello earth");
		Record r2 = new Record(456L,"hello mars");
		Record r1changed = new Record(123L,"goodbye earth" );

		test( context, getSessionFactory().withStatelessTransaction( ss -> ss
						.upsert( r1 )
						.thenCompose( v -> ss.createQuery( "from Record where message=:n", Record.class )
								.setParameter( "n", r1.message )
								.getSingleResult() )
						.thenAccept( result -> {
							assertNotNull( result );
							assertEquals( r1.message, result.message );
						} ) )
				.thenCompose( v -> getSessionFactory().withStatelessTransaction( s -> s
						.upsert( r2 )
						.thenCompose( vv -> s.refresh( r2 ) )
						.thenAccept( vv -> assertEquals( "hello mars", r2.message ) )
				) )
				// call upsert() with changed r1changed and refresh, then check for changed message
				.thenCompose( vvv -> getSessionFactory().withStatelessSession( s -> s
						.upsert(r1changed )
						.thenCompose( v -> {
							s.refresh();
							return s.createQuery( "from Record where message=:n", Record.class )
								  .setParameter( "n", r1changed.message )
								  .getSingleResult();
						} )
						  .thenAccept( result -> {
							  assertNotNull( result );
							  assertEquals( r1changed.message, result.message );
						  } ) )
				)
		);
	}

	@Test
	public void testUpsertStage(VertxTestContext context) {
		Record r1 = new Record( 123L, "hello earth" );
		Record r2 = new Record( 456L, "hello mars" );

		test( context, getSessionFactory().withStatelessTransaction( ss -> ss
						.upsert( r1 )
						.thenCompose( v -> ss.createQuery( "from Record where message=:n", Record.class )
								.setParameter( "n", r1.message )
								.getSingleResult() )
						.thenAccept( result -> {
							assertNotNull( result );
							assertEquals( r1.message, result.message );
						} ) )
				.thenCompose( v -> getSessionFactory().withStatelessTransaction( s -> s
						.upsert( r2 )
						.thenCompose( vv -> s.refresh( r2 ) )
						.thenAccept( vvv -> {
							assertEquals( "hello mars", r2.message );
							// Change r1 message
							r1.setMessage( "goodbye earth" );
						} )
				) )
				// call upsert() and refresh, then check for changed message
				.thenCompose( v -> getSessionFactory().withStatelessTransaction( s -> s
						.upsert( r1 )
						.thenCompose( vv -> s.refresh( r1 ) )
						.thenAccept( vvv -> assertEquals( "goodbye earth", r1.message ) )
				) )
		);
	}

	@Test
	public void testUpsertMutiny( VertxTestContext context) {
		Record r1 = new Record( 123L, "hello earth" );
		Record r2 = new Record( 456L, "hello mars" );

		test(
				context,
				getMutinySessionFactory()
						.withStatelessSession( ss -> ss.upsert( r1 )
								.chain( () -> ss.createQuery( "from Record where message=:n", Record.class )
										.setParameter( "n", r1.message ).getSingleResult()
										.invoke( Assertions::assertNotNull )
										.invoke( result -> assertEquals( r1.message, result.message ) )
								) )
				.call( v -> getMutinySessionFactory().withStatelessTransaction( s -> s
						.upsert( r2 )
						.call( () -> s.refresh( r2 ) )
						.invoke( () -> {
							assertEquals( "hello mars", r2.message );
							// Change r1 message
							r1.setMessage( "goodbye earth" );
						} )
				) )
				// call upsert() and refresh, then check for changed message
				.call( v -> getMutinySessionFactory().withStatelessTransaction( s -> s
						.upsert( r1 )
						.chain( () -> s.refresh( r1 ) )
						.invoke( () -> assertEquals( "goodbye earth", r1.message ) )
				) )
		);
	}

	@Entity(name = "Record")
	@Table(name = "Record")
	public static class Record {
		@Id
		public Long id;
		public String message;

		Record(Long id, String message) {
			this.id = id;
			this.message = message;
		}

		Record() {
		}

		public Long getId() {
			return id;
		}

		public String getMessage() {
			return message;
		}
		public void setMessage(String msg) {
			message = msg;
		}
	}
}
