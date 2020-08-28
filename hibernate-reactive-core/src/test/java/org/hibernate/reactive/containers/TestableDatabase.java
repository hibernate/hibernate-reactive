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

	String getUri();

}
