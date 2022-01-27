/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import javax.persistence.Id;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MYSQL;

/**
 * Check that the right exception is thrown when there is an error with the credentials.
 * <p>
 *     Similar to {@link org.hibernate.reactive.configuration.ReactiveConnectionPoolTest} but at the session level.
 *     Note that the wrong credentials are also used for the schema generation but exceptions are ignored during
 *     schema generation. You might just see some warnings in the log.
 * </p>
 */
public class WrongCredentialsTest extends BaseReactiveTest {

	private static final String BOGUS_USER = "BogusBogus";
	private static final String BOGUS_PASSWORD = "BogusBogus";

	@Rule // Sometimes, MySQL throws UnsupportedOperationException. We are not sure why.
	public DatabaseSelectionRule rule = DatabaseSelectionRule.skipTestsFor( MYSQL );

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( Settings.USER, BOGUS_USER );
		configuration.setProperty( Settings.PASS, BOGUS_PASSWORD );
		return configuration;
	}

	@Test
	public void testWithTransaction(TestContext context) {
		test( context, getSessionFactory()
				// The error will occur before the find even get executed
				.withTransaction( (s, t) -> s
						.find( Artist.class, "Banksy" ) )
				.handle( (v, e) -> {
					context.assertNotNull( e );
					context.assertTrue( isWrongCredentialsMessage( e.getMessage() ), "Error message: " + e.getMessage() );
					return null;
				} ) );
	}

	private boolean isWrongCredentialsMessage(String msg ) {
		// MySQL and PostgreSQL will contain the invalid credential value
		// Cockroach will be lower case version of the wrong user
		// Db2 will just state it
		return msg.toLowerCase().contains( BOGUS_USER.toLowerCase() )
				|| msg.contains( "Invalid credentials")
				// Oracle invalid username/password code
				|| msg.contains( "ORA-01017" );
	}

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
