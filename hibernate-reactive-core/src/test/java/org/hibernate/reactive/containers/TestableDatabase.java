/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.containers;

/**
 * A database that we use for testing.
 */
public interface TestableDatabase {

	String getJdbcUrl();

	/**
	 * Builds a prepared statement SQL string in a portable way. For example,
	 * DB2 and MySQL use syntax like "SELECT * FROM FOO WHERE BAR = ?" but
	 * PostgreSQL uses syntax like "SELECT * FROM FOO WHERE BAR = $1"
	 *
	 * @param parts The parts of the SQL not including the parameter tokens. For example:
	 * <code>statement("SELECT * FROM FOO WHERE BAR = ", "")</code>
	 */
	default String statement(String... parts) {
		return String.join( "?", parts );
	}
}
