/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
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

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;

/**
 * Check that the right exception is thrown when there is an error with the credentials.
 * <p>
 *     Similar to {@link org.hibernate.reactive.configuration.ReactiveConnectionPoolTest} but at the session level.
 *     Note that the wrong credentials are also used for the schema generation but exceptions are ignored during
 *     schema generation. You might just see some warnings in the log.
 * </p>
 */
public class WrongCredentialsTest extends BaseReactiveTest {

	// I've decided to not test it for all databases because
	// the error message might be different and it shouldn't be necessary
	@Rule
	public DatabaseSelectionRule selectionRule = DatabaseSelectionRule.runOnlyFor( POSTGRESQL );

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( Settings.USER, "BogusBogus" );
		configuration.setProperty( Settings.PASS, "BogusBogus" );
		return configuration;
	}

	@Test
	public void testWithTransaction(TestContext context) {
		test( context, getSessionFactory()
				// The error will occur before the find even get executed
				.withTransaction( (s, t) -> s
						.find( Artist.class, "Bansky" ) )
				.handle( (v, e) -> {
					context.assertNotNull( e );
					context.assertTrue( e.getMessage().contains( "BogusBogus" ) );
					return null;
				} ) );
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
