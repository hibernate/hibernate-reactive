/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.service.spi.ServiceException;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertThrown;

/**
 * Check that the right exception is thrown when there is an error when the JDBC URI scheme is not recognized.
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class WrongJdbcUrlSchemeTest extends BaseReactiveTest {

	@Override
	protected void setProperties(Configuration configuration) {
		configuration.setProperty( Settings.JAKARTA_JDBC_URL, "jdbc:h2:~/h2temp" );
		configuration.setProperty( Settings.JAKARTA_JDBC_USER, "abc" );
		setSqlLoggingProperties( configuration );
	}

	@Override
	public void before(VertxTestContext context) {
		// We need to postpone the creation of the factory so that we can check the exception
	}

	@Test
	public void testWithTransaction(VertxTestContext context) {
		test( context, assertThrown( ServiceException.class, setupSessionFactory( this::constructConfiguration ) )
				.thenAccept( WrongJdbcUrlSchemeTest::assertException )
		);
	}

	private static void assertException(Throwable throwable) {
		assertThat( throwable ).hasMessageContaining( "HR000049" );
	}

}
