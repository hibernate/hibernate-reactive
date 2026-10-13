/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;

import org.hibernate.reactive.common.AffectedEntities;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.QueryFlushMode;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The flush mode of a native mutation query is applied while executing it.
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class NativeMutationFlushModeTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Pig.class );
	}

	@Test
	public void testNativeMutationQueryFlushMode(VertxTestContext context) {
		test( context, getMutinySessionFactory().withTransaction( session -> session
				.persist( new Pig( 1, "Aloi" ) )
				.chain( () -> session.createNativeQuery( "delete from NativeFlushPig where name = :name", new AffectedEntities( Pig.class ) )
						.setFlushMode( QueryFlushMode.NO_FLUSH )
						.setParameter( "name", "Aloi" )
						.executeUpdate() )
				// the pending insert was not flushed, so there was nothing to delete
				.invoke( rows -> assertThat( rows ).isEqualTo( 0 ) )
				// control: by default the pending insert is flushed first
				.chain( () -> session.createNativeQuery( "delete from NativeFlushPig where name = :name", new AffectedEntities( Pig.class ) )
						.setParameter( "name", "Aloi" )
						.executeUpdate() )
				.invoke( rows -> assertThat( rows ).isEqualTo( 1 ) ) )
		);
	}

	@Test
	public void testNativeMutationQueryFlushWithoutAffectedEntities(VertxTestContext context) {
		// Without query spaces, the session is flushed before the query (reactively)
		test( context, getMutinySessionFactory().withTransaction( session -> session
				.persist( new Pig( 1, "Aloi" ) )
				.chain( () -> session.createNativeQuery( "delete from NativeFlushPig where name = :name" )
						.setFlushMode( QueryFlushMode.FLUSH )
						.setParameter( "name", "Aloi" )
						.executeUpdate() )
				.invoke( rows -> assertThat( rows ).isEqualTo( 1 ) ) )
		);
	}

	@Entity(name = "Pig")
	@Table(name = "NativeFlushPig")
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
