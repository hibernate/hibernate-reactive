/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.dynamic;

import io.vertx.ext.unit.TestContext;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.tuple.DynamicMapInstantiator;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DynamicEntityTest extends BaseReactiveTest {

	@Override
	protected Collection<String> mappings() {
		return List.of("org/hibernate/reactive/dynamic/Book.hbm.xml");
	}

	@Test
	public void test(TestContext context) {
		Map<String, String> book = new HashMap<>();
		book.put("ISBN", "9781932394153");
		book.put("title", "Hibernate in Action");
		book.put("author", "Christian Bauer and Gavin King");
		book.put( DynamicMapInstantiator.KEY, "Book" );

		test(
				context,
				getMutinySessionFactory()
						.withTransaction( session -> session.persist( book ) )
						.chain( v -> getMutinySessionFactory()
								.withSession( session -> session.createQuery("from Book", Map.class).getSingleResult() )
								.invoke( map -> context.assertEquals( "Christian Bauer and Gavin King", map.get("author") ) ) )
		);
	}

}
