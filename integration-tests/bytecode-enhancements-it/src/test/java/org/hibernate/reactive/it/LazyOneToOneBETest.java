/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.it;

import java.util.Collection;
import java.util.List;

import org.hibernate.reactive.it.lazytoone.Captain;
import org.hibernate.reactive.it.lazytoone.Ship;
import org.hibernate.reactive.mutiny.Mutiny;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.metamodel.Attribute;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test bytecode enhancements on a lazy one-to-one bidirectional association
 */
public class LazyOneToOneBETest extends BaseReactiveIT {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Ship.class, Captain.class );
	}

	@Test
	public void testCascadeDelete(VertxTestContext context) {
		Captain robert = new Captain( "Robert Witterel" );
		Ship obraDinn = new Ship( "Obra Dinn" );
		obraDinn.setCaptain( robert );
		robert.setShip( obraDinn );

		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persistAll( obraDinn ) )
				.chain( () -> getMutinySessionFactory()
						.withTransaction( session -> session
								.find( Ship.class, obraDinn.getId() )
								.call( session::remove ) )
				)
				.chain( () -> getMutinySessionFactory()
						.withSession( session -> session
								.find( Ship.class, obraDinn.getId() )
								.invoke( ship -> assertThat( ship ).isNull())
								.chain( () -> session.find( Captain.class, robert.getId() ) )
								.invoke( captain -> assertThat( captain ).isNull())
						)
				)
		);
	}

	@Test
	public void testFetchOnChildSide(VertxTestContext context) {
		Captain robert = new Captain( "Robert Witterel" );
		Ship obraDinn = new Ship( "Obra Dinn" );
		obraDinn.setCaptain( robert );
		robert.setShip( obraDinn );

		Attribute<? super Ship, ?> captainAttribute = getMutinySessionFactory()
				.getMetamodel().entity( Ship.class ).getAttribute( "captain" );

		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persist( obraDinn ) )
				.chain( () -> getMutinySessionFactory()
						.withSession( session -> session
								.find( Ship.class, obraDinn.getId() )
								.call( ship -> session.fetch( ship, captainAttribute ) )
								.invoke( ship -> {
									assertThat( ship ).isEqualTo( obraDinn );
									assertThat( ship.getCaptain() ).isEqualTo( robert );
								} )
						)
				)
		);
	}

	@Test
	public void testFetchOnParentSide(VertxTestContext context) {
		Captain robert = new Captain( "Robert Witterel" );
		Ship obraDinn = new Ship( "Obra Dinn" );
		obraDinn.setCaptain( robert );
		robert.setShip( obraDinn );

		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persistAll( obraDinn ) )
				.chain( () -> getMutinySessionFactory()
						.withSession( session -> session
								.find( Captain.class, robert.getId() )
								.invoke( captain -> assertThat( captain ).isEqualTo( robert ) )
								.chain( captain -> Mutiny.fetch( captain.getShip() ) )
								.invoke( ship -> assertThat( ship ).isEqualTo( obraDinn ) )
						)
				)
		);
	}
}
