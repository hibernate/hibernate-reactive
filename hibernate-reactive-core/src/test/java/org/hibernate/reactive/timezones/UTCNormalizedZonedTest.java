/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.timezones;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;

import org.junit.Test;

import io.vertx.ext.unit.TestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.cfg.AvailableSettings.TIMEZONE_DEFAULT_STORAGE;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertWithTruncationThat;
import static org.hibernate.type.descriptor.DateTimeUtils.roundToDefaultPrecision;

public class UTCNormalizedZonedTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Zoned.class );
	}

	@Override
	protected void setProperties(Configuration configuration) {
		super.setProperties( configuration );
		configuration.setProperty( TIMEZONE_DEFAULT_STORAGE, "NORMALIZE_UTC" );
	}

	@Test
	public void test(TestContext context) {
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
									assertThat( z.offsetDateTime.getOffset() ).isEqualTo( ZoneId.of( "Z" ) );
									assertThat( z.zonedDateTime.getZone() ).isEqualTo( ZoneOffset.ofHours( 0 ) );
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
