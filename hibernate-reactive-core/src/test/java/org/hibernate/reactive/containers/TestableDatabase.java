/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
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
