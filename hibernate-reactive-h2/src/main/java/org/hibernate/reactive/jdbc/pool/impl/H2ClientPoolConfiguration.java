/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import java.net.URI;

import io.vertx.core.json.JsonObject;

public class H2ClientPoolConfiguration extends DefaultSqlClientPoolConfiguration implements JdbcClientPoolConfiguration {

	@Override
	public JsonObject jdbcConnectOptions(URI uri) {
		return new JsonObject()
				.put( "url", uri.toString() )
				.put( "user", "sa" )
				.put( "pass", null );
	}
}
