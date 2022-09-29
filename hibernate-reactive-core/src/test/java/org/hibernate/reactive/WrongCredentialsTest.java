/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.provider.Settings;

import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Check that the right exception is thrown when there is an error with the credentials.
 * <p>
 *     Similar to {@link org.hibernate.reactive.configuration.ReactiveConnectionPoolTest} but at the session level.
 *     Note that the wrong credentials are also used for the schema generation but exceptions are ignored during
 *     schema generation. You might just see some warnings in the log.
 * </p>
 */
public class WrongCredentialsTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Artist.class );
		configuration.setProperty( Settings.USER, DatabaseConfiguration.USERNAME );
		configuration.setProperty( Settings.PASS, "BogusBogus" );
		return configuration;
	}

	@Test
	public void testWithTransaction(TestContext context) {
		test( context, getSessionFactory()
				// The error will occur before the find even get executed
				.withTransaction( s -> s.find( Artist.class, "Banksy" ) )
				.handle( (v, e) -> e )
				.thenAccept( WrongCredentialsTest::assertException )
		);
	}

	private static void assertException(Throwable throwable) {
		assertThat( throwable ).as( "We were expecting an exception" ).isNotNull();
		assertThat( expectedMessage( throwable.getMessage() ) )
				.as( "Unexpected error message: " + throwable.getMessage() )
				.isTrue();
	}

	private static boolean expectedMessage(String msg) {
		final String lowerCaseMsg = msg.toLowerCase();
		return lowerCaseMsg.contains( "password authentication failed" )
				|| lowerCaseMsg.contains( "login failed" )
				|| lowerCaseMsg.contains( "access denied" )
				|| lowerCaseMsg.contains( "invalid credentials")
				// Oracle invalid username/password code
				|| msg.contains( "ORA-01017" );
	}

	@Entity
	static class Artist {
		@Id
		String name;

		public Artist() {
		}

		public Artist(String name) {
			this.name = name;
		}
	}
}
