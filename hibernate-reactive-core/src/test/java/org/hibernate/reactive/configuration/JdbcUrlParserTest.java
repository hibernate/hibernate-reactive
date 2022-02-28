/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.configuration;

import org.hibernate.HibernateError;
import org.hibernate.reactive.pool.impl.DefaultSqlClientPool;
import org.hibernate.reactive.pool.impl.DefaultSqlClientPoolConfiguration;

import org.junit.Assert;
import org.junit.Test;

import java.net.URI;

import io.vertx.sqlclient.SqlConnectOptions;

import static org.assertj.core.api.Assertions.assertThat;

public class JdbcUrlParserTest {

	@Test
	public void returnsNullForNull() {
		Assert.assertThrows(HibernateError.class, () -> {
			URI uri = DefaultSqlClientPool.parse( null );
			assertThat( uri ).isNull();
		});
	}

	@Test
	public void invalidParameter() {
		Assert.assertThrows(HibernateError.class, () -> {
			URI uri = DefaultSqlClientPool.parse( "jdbc:postgresql://localhost:5432/hreact");
			DefaultSqlClientPoolConfiguration cfg = new DefaultSqlClientPoolConfiguration();
			final SqlConnectOptions connectOptions = cfg.connectOptions( uri );
		});
	}

	@Test
	public void parameters() {
		URI uri = DefaultSqlClientPool.parse( "jdbc:postgresql://localhost:5432/hreact?user=hello");
		DefaultSqlClientPoolConfiguration cfg = new DefaultSqlClientPoolConfiguration();
		final SqlConnectOptions connectOptions = cfg.connectOptions( uri );
		assertThat(connectOptions).isNotNull();
	}

	@Test
	public void uriCreation() {
		URI uri = DefaultSqlClientPool.parse( "jdbc:postgresql://localhost:5432/hreact");
		assertThat(uri).isNotNull();
	}

	@Test
	public void parsePort() {
		URI uri = DefaultSqlClientPool.parse( "jdbc:postgresql://localhost:5432/hreact");
		assertThat(uri).hasPort(5432);
	}

	@Test
	public void parseHost() {
		URI uri = DefaultSqlClientPool.parse( "jdbc:postgresql://localhost:5432/hreact");
		assertThat(uri).hasHost("localhost");
	}

	@Test
	public void parseScheme() {
		URI uri = DefaultSqlClientPool.parse( "jdbc:postgresql://localhost:5432/hreact");
		assertThat(uri).hasScheme("postgresql");
	}
}
