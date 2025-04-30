/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.timezones;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.cfg.AvailableSettings.JDBC_TIME_ZONE;
import static org.hibernate.cfg.AvailableSettings.TIMEZONE_DEFAULT_STORAGE;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertWithTruncationThat;
import static org.hibernate.type.descriptor.DateTimeUtils.adjustToDefaultPrecision;

/**
 * Test adapted from {@link org.hibernate.orm.test.timezones.JDBCTimeZoneZonedTest}
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class JDBCTimeZoneZonedTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Zoned.class );
	}

	@Override
	protected void setProperties(Configuration configuration) {
		super.setProperties( configuration );
		configuration.setProperty( TIMEZONE_DEFAULT_STORAGE, "NORMALIZE" );
		configuration.setProperty( JDBC_TIME_ZONE, "GMT+5" );
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

									ZoneId systemZone = ZoneId.systemDefault();
									ZoneOffset systemOffset = systemZone.getRules().getOffset( Instant.now() );
									assertThat( z.zonedDateTime.getZone() ).isEqualTo( systemZone );
									assertThat( z.offsetDateTime.getOffset() ).isEqualTo( systemOffset );
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
