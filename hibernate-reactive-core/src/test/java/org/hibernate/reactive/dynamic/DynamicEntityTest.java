/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.dynamic;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.reactive.BaseReactiveTest;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;

import static java.util.Map.entry;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 10, timeUnit = MINUTES)

public class DynamicEntityTest extends BaseReactiveTest {

	@Override
	protected Collection<String> mappings() {
		return List.of( "org/hibernate/reactive/dynamic/Book.hbm.xml" );
	}

	@Test
	public void test(VertxTestContext context) {
		Map<String, String> book = new HashMap<>();
		book.put( "ISBN", "9781932394153" );
		book.put( "title", "Hibernate in Action" );
		book.put( "author", "Christian Bauer and Gavin King" );

		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persist( book ) )
				.chain( v -> getMutinySessionFactory()
						.withSession( session -> session
								.createSelectionQuery( "from Book", Map.class )
								.getSingleResult() )
						.invoke( map -> {
							assertThat( map ).containsExactly(
									entry( "author", "Christian Bauer and Gavin King" ),
									entry( "ISBN", "9781932394153" ),
									entry( "title", "Hibernate in Action" ),
									entry( "author", "Christian Bauer and Gavin King" )
							);
						} ) )
		);
	}

}
