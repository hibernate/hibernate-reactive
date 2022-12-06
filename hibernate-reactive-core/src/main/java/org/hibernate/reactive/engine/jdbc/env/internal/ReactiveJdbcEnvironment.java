/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.env.internal;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.env.internal.JdbcEnvironmentImpl;
import org.hibernate.service.spi.ServiceRegistryImplementor;

public class ReactiveJdbcEnvironment extends JdbcEnvironmentImpl {

	public ReactiveJdbcEnvironment(ServiceRegistryImplementor registry, Dialect dialect) {
		super( registry, dialect );
	}

	@Deprecated
	public ReactiveJdbcEnvironment(ServiceRegistryImplementor registry, Dialect dialect, DatabaseMetaData metaData) throws SQLException {
		super( registry, dialect, metaData );
	}
}
