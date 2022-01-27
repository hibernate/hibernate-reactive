/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.exception;

import java.sql.SQLException;

/**
 * the requested DML operation resulted in a violation of a defined integrity constraint.
 *
 * @see org.hibernate.exception.ConstraintViolationException
 */
public class ConstraintViolationException extends VertxSqlClientException {

	public ConstraintViolationException(String message, SQLException root, String constraintName) {
		super( message, root );
	}

	public ConstraintViolationException(String message, SQLException root, String sql, String constraintName) {
		super( message, root, sql );
	}
}
