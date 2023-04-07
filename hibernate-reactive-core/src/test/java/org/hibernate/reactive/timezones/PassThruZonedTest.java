/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.timezones;

import io.vertx.ext.unit.TestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.type.descriptor.DateTimeUtils;
import org.junit.Test;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;

import static org.hibernate.cfg.AvailableSettings.TIMEZONE_DEFAULT_STORAGE;

public class PassThruZonedTest extends BaseReactiveTest {
	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of(Zoned.class);
	}

	@Override
	protected void setProperties(Configuration configuration) {
		super.setProperties(configuration);
		configuration.setProperty(TIMEZONE_DEFAULT_STORAGE, "NORMALIZE");
	}

	@Test
	public void test(TestContext context) {
		ZonedDateTime nowZoned = ZonedDateTime.now().withZoneSameInstant( ZoneId.of("CET") );
		OffsetDateTime nowOffset = OffsetDateTime.now().withOffsetSameInstant( ZoneOffset.ofHours(3) );
		test( context, getSessionFactory()
				.withTransaction( s -> {
					Zoned z = new Zoned();
					z.zonedDateTime = nowZoned;
					z.offsetDateTime = nowOffset;
					return s.persist( z ).thenApply( v -> z.id );
				} )
				.thenCompose( zid -> openSession()
						.thenCompose( s -> s.find( Zoned.class, zid )
								.thenAccept( z-> {
									context.assertEquals(
											DateTimeUtils.roundToDefaultPrecision( nowZoned.toInstant(), getDialect() ),
											DateTimeUtils.roundToDefaultPrecision( z.zonedDateTime.toInstant(), getDialect() )
									);
									context.assertEquals(
											DateTimeUtils.roundToDefaultPrecision( nowOffset.toInstant(), getDialect() ),
											DateTimeUtils.roundToDefaultPrecision( z.offsetDateTime.toInstant(), getDialect() )
									);
									ZoneId systemZone = ZoneId.systemDefault();
									ZoneOffset systemOffset = systemZone.getRules().getOffset( Instant.now() );
									context.assertEquals( systemZone, z.zonedDateTime.getZone() );
									context.assertEquals( systemOffset, z.offsetDateTime.getOffset() );
								} )
						)
				)
		);
	}

	@Entity(name = "Zoned")
	public static class Zoned {
		@Id
		@GeneratedValue Long id;
		ZonedDateTime zonedDateTime;
		OffsetDateTime offsetDateTime;
	}
}
