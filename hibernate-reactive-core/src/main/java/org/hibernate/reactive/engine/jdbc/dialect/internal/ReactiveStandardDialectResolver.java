/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.dialect.internal;

import org.hibernate.dialect.CockroachDialect;
import org.hibernate.dialect.Database;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolver;

import static org.hibernate.dialect.CockroachDialect.parseVersion;

public class ReactiveStandardDialectResolver implements DialectResolver {

	@Override
	public Dialect resolveDialect(DialectResolutionInfo info) {
		// Hibernate ORM runs an extra query to recognize CockroachDB from PostgreSQL
		// We've already done it, so we are trying to skip that step
		if ( info.getDatabaseName().startsWith( "Cockroach" ) ) {
			return new CockroachDialect( parseVersion( info.getDatabaseVersion() ) );
		}

		for ( Database database : Database.values() ) {
			if ( database.matchesResolutionInfo( info ) ) {
				return database.createDialect( info );
			}
		}

		return null;
	}
}
