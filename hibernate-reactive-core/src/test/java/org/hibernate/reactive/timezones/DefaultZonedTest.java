/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.timezones;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
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
import static org.hibernate.reactive.testing.ReactiveAssertions.assertWithTruncationThat;
import static org.hibernate.type.descriptor.DateTimeUtils.adjustToDefaultPrecision;

/**
 * Test adapted from {@link org.hibernate.orm.test.timezones.DefaultZonedTest}
 */
@Timeout(value = 10, timeUnit = MINUTES)
@DisabledFor(value = DB2, reason = "Exception: IllegalStateException: Needed to have 6 in buffer but only had 0")
public class DefaultZonedTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Zoned.class );
	}

	@Test
	public void test(VertxTestContext context) {
		final ZonedDateTime nowZoned;
		final OffsetDateTime nowOffset;
		if ( getDialect().getDefaultTimestampPrecision() == 6 ) {
			nowZoned = ZonedDateTime.now().withZoneSameInstant( ZoneId.of("CET") ).truncatedTo( ChronoUnit.MICROS );
			nowOffset = OffsetDateTime.now().withOffsetSameInstant( ZoneOffset.ofHours(3) ).truncatedTo( ChronoUnit.MICROS );
		}
		else {
			nowZoned = ZonedDateTime.now().withZoneSameInstant( ZoneId.of("CET") );
			nowOffset = OffsetDateTime.now().withOffsetSameInstant( ZoneOffset.ofHours(3) );
		}
		test( context, getSessionFactory()
				.withTransaction( s -> {
					Zoned z = new Zoned();
					z.zonedDateTime = nowZoned;
					z.offsetDateTime = nowOffset;
					return s.persist( z ).thenApply( v -> z.id );
				} )
				.thenCompose( zid -> openSession()
						.thenCompose( s -> s.find( Zoned.class, zid )
								.thenAccept( z -> {
									assertWithTruncationThat( adjustToDefaultPrecision( z.zonedDateTime.toInstant(), getDialect() ) )
													.isEqualTo( adjustToDefaultPrecision( nowZoned.toInstant(), getDialect() ) );
									assertWithTruncationThat( adjustToDefaultPrecision( z.offsetDateTime.toInstant(), getDialect() ) )
											.isEqualTo( adjustToDefaultPrecision( nowOffset.toInstant(), getDialect() ) );

									if ( getDialect().getTimeZoneSupport() == TimeZoneSupport.NATIVE ) {
										assertThat( z.zonedDateTime.toOffsetDateTime().getOffset() )
												.isEqualTo( nowZoned.toOffsetDateTime().getOffset() );
										assertThat( z.offsetDateTime.getOffset() ).isEqualTo( nowOffset.getOffset() );
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
