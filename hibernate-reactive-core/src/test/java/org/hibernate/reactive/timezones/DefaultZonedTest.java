/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.timezones;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

import org.hibernate.dialect.TimeZoneSupport;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.testing.DBSelectionExtension;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertWithTruncationThat;
import static org.hibernate.type.descriptor.DateTimeUtils.roundToDefaultPrecision;

@Timeout(value = 10, timeUnit = MINUTES)

public class DefaultZonedTest extends BaseReactiveTest {

	// Db2: Exception: IllegalStateException: Needed to have 6 in buffer but only had 0
	@RegisterExtension
	public final DBSelectionExtension skip = DBSelectionExtension.skipTestsFor( DB2 );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Zoned.class );
	}

	@Test
	public void test(VertxTestContext context) {
		ZonedDateTime nowZoned = ZonedDateTime.now().withZoneSameInstant( ZoneId.of( "CET" ) );
		OffsetDateTime nowOffset = OffsetDateTime.now().withOffsetSameInstant( ZoneOffset.ofHours( 3 ) );
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
									assertWithTruncationThat( roundToDefaultPrecision( z.zonedDateTime.toInstant(), getDialect() ) )
													.isEqualTo( roundToDefaultPrecision( nowZoned.toInstant(), getDialect() ) );
									assertWithTruncationThat( roundToDefaultPrecision( z.offsetDateTime.toInstant(), getDialect() ) )
											.isEqualTo( roundToDefaultPrecision( nowOffset.toInstant(), getDialect() ) );

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
