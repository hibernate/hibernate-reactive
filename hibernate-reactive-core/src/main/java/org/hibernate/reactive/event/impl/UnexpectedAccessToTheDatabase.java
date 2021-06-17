/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.impl;

/**
 * For situation where we don't excpect to access the database,
 * <p>
 *     For Reactive, this usually means that we are returning a {@link java.util.concurrent.CompletionStage}
 *     and we are not ready to handle it.
 * </p>
 */
public class UnexpectedAccessToTheDatabase extends RuntimeException {

	public UnexpectedAccessToTheDatabase() {
		this( "Unexpected access to the database" );
	}

	public UnexpectedAccessToTheDatabase(String message) {
		super( message );
	}
}
