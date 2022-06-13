/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.pool.impl.DefaultSqlClientPoolConfiguration;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import io.vertx.sqlclient.SqlConnectOptions;
import org.assertj.core.api.Assertions;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.H2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

/**
 * Test the default port is set correctly when using {@link DefaultSqlClientPoolConfiguration}
 */
public class DefaultPortTest {

	@Rule
	public DatabaseSelectionRule dbRule = DatabaseSelectionRule.skipTestsFor( H2 );

	@Test
	public void testDefaultPortIsSet() throws URISyntaxException {
		DefaultSqlClientPoolConfiguration configuration = new DefaultSqlClientPoolConfiguration();
		configuration.configure( requiredProperties() );
		SqlConnectOptions sqlConnectOptions = configuration.connectOptions( new URI( scheme() + "://localhost/database" ) );
		Assertions.assertThat( sqlConnectOptions.getPort() )
				.as( "Default port not defined for " + dbType() )
				.isEqualTo( dbType().getDefaultPort() );
	}

	@Test
	public void testUnrecognizedSchemeException() throws URISyntaxException {
		Assert.assertThrows( IllegalArgumentException.class, () -> {
			URI uri = new URI( "bogusScheme://localhost/database" );
			new DefaultSqlClientPoolConfiguration().connectOptions( uri );
		} );
	}

	private static String scheme() {
		if ( dbType() == DatabaseConfiguration.DBType.MARIA ) {
			return "mariadb";
		}
		return dbType().name().toLowerCase( Locale.ROOT );
	}

	// Only set the required properties
	// We are not connecting to a db so it doesn't matter
	private Map requiredProperties() {
		Map map = new HashMap();
		map.put( Settings.USER, "BogusUser" );
		return map;
	}
}
