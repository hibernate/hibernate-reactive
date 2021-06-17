/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.common;

/**
 * Represents the shape of a native SQL query result
 * set, as specified by an instance of the annotation
 * {@link javax.persistence.SqlResultSetMapping}. At
 * runtime an instance may be obtained by calling
 * {@code session.getResultSetMapping(type, name)}, or
 * using a typesafe reference produced by some sort of
 * compile-time code generator.
 *
 * @see org.hibernate.reactive.mutiny.Mutiny.Session#getResultSetMapping(Class, String)
 * @see org.hibernate.reactive.stage.Stage.Session#getResultSetMapping(Class, String)
 *
 * @param <T> the Java result type of the query
 */
public interface ResultSetMapping<T> {
	/**
	 * The name of the result set mapping, as specified by
	 * {@link javax.persistence.SqlResultSetMapping#name}.
	 */
	String getName();

	/**
	 * The Java result type of the query. For queries
	 * which return multiple columns of results in each
	 * row, this must be {@code Object[]}.
	 */
	Class<T> getResultType();
}
