/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import org.hibernate.FlushMode;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;

import static org.assertj.core.api.Assertions.assertThat;

public class NoEntitiesTest extends BaseReactiveTest {

	@Test
	public void emptyMetamodelWithMutiny() {
		assertThat( getMutinySessionFactory().getMetamodel().getEntities() ).isEmpty();
	}

	@Test
	public void shouldBeAbleToRunQueryWithMutinyTransaction(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s
						.createNativeQuery( "select 42", Long.class ).getSingleResult()
				).invoke( result -> assertThat( result ).isEqualTo( 42L ) )
		);
	}

	@Test
	public void runNativeQueryWithMutinyTransactionAndFlush(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( s -> {
					s.setFlushMode( FlushMode.ALWAYS );
					return s
							.createNativeQuery( "select 42", Long.class ).getSingleResult()
							.call( s::flush );
				} ).invoke( result -> assertThat( result ).isEqualTo( 42L ) )
		);
	}

	@Test
	public void runStatelessNativeQueryWithMutinyTransactionAndFlush(VertxTestContext context) {
		test(
				context, getMutinySessionFactory()
						.withStatelessTransaction( s -> s
								.createNativeQuery( "select 42", Long.class )
								.getSingleResult()
						)
						.invoke( result -> assertThat( result ).isEqualTo( 42L ) )
		);
	}

	@Test
	public void shouldBeAbleToRunQueryWithMutinyWithoutTransaction(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.openSession().chain( s -> s
						.createNativeQuery( "select 666", Long.class ).getSingleResult()
				).invoke( result -> assertThat( result ).isEqualTo( 666L ) )
		);
	}
}
