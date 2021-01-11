/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.concurrent.CompletionException;
import javax.persistence.Entity;
import javax.persistence.Id;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.provider.Settings;

import org.junit.Test;

import io.vertx.db2client.DB2Exception;
import io.vertx.ext.unit.TestContext;
import io.vertx.mysqlclient.MySQLException;
import io.vertx.pgclient.PgException;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;

/**
 * Check that the right exception is thrown when there is an error with the credentials.
 * <p>
 *     Similar to {@link org.hibernate.reactive.configuration.ReactiveConnectionPoolTest} but at the session level.
 *     Note that the wrong configuration is also used for the schema generation but ORM catch the exception and
 *     continue anyway it in case of errors. You might just see some warnings in the log.
 * </p>
 */
public class WrongCredentialsTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		// We need to set both credentials or they are going to be ignored
		// See DefaultSqlClientPoolConfiguration
		configuration.setProperty( Settings.USER, "BogusBogus" );
		configuration.setProperty( Settings.PASS, "BogusBogus" );
		configuration.addAnnotatedClass( Artist.class );
		return configuration;
	}

	@Test
	public void testStageWithTransaction(TestContext context) {
		test( context, getSessionFactory()
				// The error will occur before the find even get executed
				.withTransaction( (s, t) -> s.find( Artist.class, "Bansky" ) )
				.handle( (v, e) -> {
					context.assertNotNull( e );
					context.assertEquals( CompletionException.class, e.getClass() );
					switch ( DatabaseConfiguration.dbType() ) {
						case DB2:
							context.assertEquals( DB2Exception.class, e.getCause().getClass()  );
							context.assertTrue( e.getMessage().contains( "Invalid credentials" ) );
							break;
						case POSTGRESQL:
							context.assertEquals( PgException.class, e.getCause().getClass()  );
							context.assertTrue( e.getMessage().contains( "BogusBogus" ) );
							break;
						case MYSQL:
							context.assertEquals( MySQLException.class, e.getCause().getClass()  );
							context.assertTrue( e.getMessage().contains( "BogusBogus" ) );
							break;
						default:
							context.fail( "Assertions missing for " + DatabaseConfiguration.dbType() );
					}
					return null;
				} ) );
	}

	@Test
	public void testMutinyWithTransaction(final TestContext context) {
		test( context, getMutinySessionFactory()
				// The error will occur before the find even get executed
				.withTransaction( (s, t) -> s.find( Artist.class, "Bansky" ) )
				.onItem().invoke( a -> context.fail( "Expected an exception" ) )
				.onFailure().recoverWithItem( e -> {
					switch ( DatabaseConfiguration.dbType() ) {
						case DB2:
							context.assertEquals( DB2Exception.class, e.getClass()  );
							context.assertTrue( e.getMessage().contains( "Invalid credentials" ) );
							break;
						case POSTGRESQL:
							context.assertEquals( PgException.class, e.getClass() );
							context.assertTrue( e.getMessage().contains( "BogusBogus" ) );
							break;
						case MYSQL:
							context.assertEquals( MySQLException.class, e.getClass() );
							context.assertTrue( e.getMessage().contains( "BogusBogus" ) );
							break;
						default:
							context.fail( "Assertion missing for " + DatabaseConfiguration.dbType() );
					}
					return null;
				} )
		);
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
