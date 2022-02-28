/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceConfigurationError;

import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.pool.impl.DefaultSqlClientPoolConfiguration;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.vertx.sqlclient.SqlConnectOptions;
import org.assertj.core.api.Assertions;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

/**
 * Test the default port is set correctly when using {@link DefaultSqlClientPoolConfiguration}
 */
public class DefaultPortTest {

	@Rule
	public DatabaseSelectionRule skipDbs = DatabaseSelectionRule.skipTestsFor( ORACLE );

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testDefaultPortIsSet() throws URISyntaxException {
		DefaultSqlClientPoolConfiguration configuration = new DefaultSqlClientPoolConfiguration();
		configuration.configure( requiredProperties() );
		SqlConnectOptions sqlConnectOptions = configuration.connectOptions( new URI( uriString() ) );
		Assertions.assertThat( sqlConnectOptions.getPort() )
				.as( "Default port not defined for " + dbType() )
				.isEqualTo( dbType().getDefaultPort() );
	}

	@Test
	public void testUnrecognizedSchemeException() throws URISyntaxException {
		thrown.expect( ServiceConfigurationError.class );

		URI uri = new URI( "bogusScheme://localhost/database" );
		new DefaultSqlClientPoolConfiguration().connectOptions( uri );
	}

	private static String uriString() {
		if ( dbType() == DatabaseConfiguration.DBType.MARIA ) {
			return "mariadb://localhost/database";
		}
		if ( dbType() == ORACLE ) {
			return "oracle:thin:@localhost:orcl";
		}
		return dbType().name().toLowerCase() + "://localhost/database";
	}

	// Only set the required properties
	// We are not connecting to a db so it doesn't matter
	private Map requiredProperties() {
		Map map = new HashMap();
		map.put( Settings.USER, "BogusUser" );
		return map;
	}
}
