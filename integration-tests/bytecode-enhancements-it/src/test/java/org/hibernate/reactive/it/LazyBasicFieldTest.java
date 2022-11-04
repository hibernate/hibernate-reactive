/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.it;

import java.util.Collection;
import java.util.List;

import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test fetching of basic lazy fields when
 * bytecode enhancements is enabled.
 */
public class LazyBasicFieldTest extends BaseReactiveIT {
	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Crew.class );
	}

	@Test
	public void testFetchBasicField(TestContext context) {
		final Crew emily = new Crew();
		emily.setId( 21L );
		emily.setName( "Emily Jackson" );
		emily.setRole( "Passenger" );
		emily.setFate( "Unknown" );

		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persist( emily ) )
				.chain( () -> getMutinySessionFactory()
						.withSession( session -> session
								.find( Crew.class, emily.getId() )
								.call( crew -> session.fetch( crew, Crew_.role )
										.invoke( role -> assertThat( role ).isEqualTo( emily.getRole() ) ) )
								.call( crew -> session.fetch( crew, Crew_.fate )
										.invoke( fate -> assertThat( fate ).isEqualTo( emily.getFate() ) )
								) ) )
		);
	}
}
