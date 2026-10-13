/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;

import org.hibernate.reactive.annotations.DisabledFor;
import org.hibernate.reactive.mutiny.Mutiny;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.COCKROACHDB;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertThrown;

/**
 * The execution of an HQL mutation query behaves like
 * {@link org.hibernate.query.internal.AbstractQuery#executeMutation} in ORM.
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class MutationQueryExecutionTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Pig.class );
	}

	@Test
	@DisabledFor(value = { POSTGRESQL, COCKROACHDB }, reason = "The conflict clause with a constraint name is supported")
	public void testIllegalQueryOperationIsConvertedToIllegalStateException(VertxTestContext context) {
		// ORM throws an IllegalQueryOperationException when it cannot render the conflict clause
		test( context, assertThrown(
				IllegalStateException.class,
				getMutinySessionFactory().withTransaction( session -> session
						.createMutationQuery( "insert into Pig (id, name) values (1, 'Aloi') "
								+ "on conflict on constraint pig_pk do update set name = excluded.name" )
						.executeUpdate() )
		) );
	}

	@Test
	public void testExecuteTwiceWithDifferentListParameters(VertxTestContext context) {
		test( context, getMutinySessionFactory().withTransaction( session -> session
				.persistAll( new Pig( 1, "Aloi" ), new Pig( 2, "Xavier" ), new Pig( 3, "Babe" ) )
				.chain( session::flush )
				.chain( () -> {
					final Mutiny.MutationQuery query = session.createMutationQuery( "delete from Pig where id in :ids" );
					return query
							.setParameter( "ids", List.of( 1, 2 ) )
							.executeUpdate()
							.invoke( rows -> assertThat( rows ).isEqualTo( 2 ) )
							// the expansion of the previous execution must not be reused
							.chain( () -> query.setParameter( "ids", List.of( 3 ) ).executeUpdate() )
							.invoke( rows -> assertThat( rows ).isEqualTo( 1 ) );
				} ) )
		);
	}

	@Entity(name = "Pig")
	@Table(name = "MutationExecutionPig")
	public static class Pig {
		@Id
		Integer id;
		String name;

		public Pig() {
		}

		public Pig(Integer id, String name) {
			this.id = id;
			this.name = name;
		}
	}
}
