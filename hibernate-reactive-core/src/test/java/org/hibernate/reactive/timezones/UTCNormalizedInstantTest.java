/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.timezones;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.TimeZone;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.reactive.BaseReactiveTest;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

import static java.sql.Types.TIMESTAMP;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertWithTruncationThat;

@Timeout(value = 10, timeUnit = MINUTES)

public class UTCNormalizedInstantTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Zoned.class );
	}

	final Instant instant = Instant.now();

	@Test
	public void test(VertxTestContext scope) {
		test( scope, getMutinySessionFactory()
				.withTransaction( s -> {
					Zoned z = new Zoned();
					z.utcInstant = instant;
					z.localInstant = instant;
					return s.persist( z ).replaceWith( z );
				} )
				.call( zz -> getMutinySessionFactory()
						.withSession( s -> s.find( Zoned.class, zz.id ) )
						.invoke( this::assertInstant )
				)
		);
	}

	@Test
	public void testWithSystemTimeZone(VertxTestContext scope) {
		TimeZone.setDefault( TimeZone.getTimeZone( "CET" ) );
		test( scope, getMutinySessionFactory()
				.withTransaction( s -> {
					Zoned z = new Zoned();
					z.utcInstant = instant;
					z.localInstant = instant;
					return s.persist( z ).replaceWith( z );
				} )
				.call( zz -> getMutinySessionFactory()
						.withSession( s -> s.find( Zoned.class, zz.id ) )
						.invoke( this::assertInstant )
				)
		);
	}

	private void assertInstant(Zoned z) {
		assertWithTruncationThat( z.utcInstant ).isEqualTo( instant );
		assertWithTruncationThat( z.localInstant ).isEqualTo( instant );
	}

	@Entity(name = "Zoned")
	public static class Zoned {
		@Id
		@GeneratedValue
		Long id;
		Instant utcInstant;
		@JdbcTypeCode(TIMESTAMP)
		Instant localInstant;
	}
}
