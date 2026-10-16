/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.timezones;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;

import org.hibernate.dialect.TimeZoneSupport;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.annotations.DisabledFor;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;

/**
 * Test adapted from {@link org.hibernate.orm.test.timezones.DefaultZonedTest}
 */
@Timeout(value = 10, timeUnit = MINUTES)
@DisabledFor(value = DB2, reason = "Exception: IllegalStateException: Needed to have 6 in buffer but only had 0")
public class DefaultZonedTest extends BaseReactiveTest {

	// A fixed value with millisecond precision is stored exactly by every supported database,
	// so the result doesn't depend on whether the database rounds or truncates fractional seconds
	private static final Instant INSTANT = Instant.parse( "2024-03-15T10:15:30.123Z" );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Zoned.class );
	}

	@Test
	public void test(VertxTestContext context) {
		final ZonedDateTime zoned = INSTANT.atZone( ZoneId.of( "CET" ) );
		final OffsetDateTime offset = INSTANT.atOffset( ZoneOffset.ofHours( 3 ) );
		test( context, getSessionFactory()
				.withTransaction( s -> {
					Zoned z = new Zoned();
					z.zonedDateTime = zoned;
					z.offsetDateTime = offset;
					return s.persist( z ).thenApply( v -> z.id );
				} )
				.thenCompose( zid -> openSession()
						.thenCompose( s -> s.find( Zoned.class, zid )
								.thenAccept( z -> {
									assertThat( z.zonedDateTime.toInstant() ).isEqualTo( INSTANT );
									assertThat( z.offsetDateTime.toInstant() ).isEqualTo( INSTANT );

									if ( getDialect().getTimeZoneSupport() == TimeZoneSupport.NATIVE ) {
										assertThat( z.zonedDateTime.toOffsetDateTime().getOffset() )
												.isEqualTo( zoned.toOffsetDateTime().getOffset() );
										assertThat( z.offsetDateTime.getOffset() ).isEqualTo( offset.getOffset() );
									}
									else {
										assertThat( z.zonedDateTime.getZone() )
												.isEqualTo( ZoneId.of( "Z" ) );
										assertThat( z.offsetDateTime.getOffset() )
												.isEqualTo( ZoneOffset.ofHours( 0 ) );
									}
								} )
						)
				)
		);
	}

	@Entity(name = "Zoned")
	public static class Zoned {
		@Id
		@GeneratedValue
		Long id;
		ZonedDateTime zonedDateTime;
		OffsetDateTime offsetDateTime;
	}
}
