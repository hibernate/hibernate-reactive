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

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
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
import static org.hibernate.cfg.AvailableSettings.JDBC_TIME_ZONE;
import static org.hibernate.cfg.AvailableSettings.TIMEZONE_DEFAULT_STORAGE;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;

/**
 * Test adapted from {@link org.hibernate.orm.test.timezones.JDBCTimeZoneZonedTest}
 */
@Timeout(value = 10, timeUnit = MINUTES)
@DisabledFor(value = DB2, reason = "Exception: SQLException: An error occurred with a DB2 operation, SQLCODE=-180  SQLSTATE=22007")
public class JDBCTimeZoneZonedTest extends BaseReactiveTest {

	// A fixed value with millisecond precision is stored exactly by every supported database,
	// so the result doesn't depend on whether the database rounds or truncates fractional seconds
	private static final Instant INSTANT = Instant.parse( "2024-03-15T10:15:30.123Z" );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Zoned.class );
	}

	@Override
	protected void setProperties(Configuration configuration) {
		super.setProperties( configuration );
		configuration.setProperty( TIMEZONE_DEFAULT_STORAGE, "NORMALIZE" );
		configuration.setProperty( JDBC_TIME_ZONE, "GMT+5" );
		configuration.setProperty( AvailableSettings.JAVA_TIME_USE_DIRECT_JDBC, "false" );
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

									ZoneId systemZone = ZoneId.systemDefault();
									ZoneOffset systemOffset = systemZone.getRules().getOffset( INSTANT );
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
