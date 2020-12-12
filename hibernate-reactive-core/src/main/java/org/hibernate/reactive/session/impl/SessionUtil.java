/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class SessionUtil {

	public static void throwEntityNotFound(SessionImplementor session, String entityName, Serializable identifier) {
		session.getFactory().getEntityNotFoundDelegate().handleEntityNotFound( entityName, identifier );
	}

	public static void checkEntityFound(SessionImplementor session, String entityName, Serializable identifier, Object optional) {
		if ( optional==null ) {
			throwEntityNotFound(session, entityName, identifier);
		}
	}

	public static <R> CompletionStage<R> wrapReactive(ReactiveConnectionSupplier reactiveConnection, Function<ReactiveConnection, CompletionStage<R>> operation) {
		return wrapReactive( reactiveConnection.getReactiveConnection(), operation );
	}

	public static <R> CompletionStage<R> wrapReactive(ReactiveConnection reactiveConnection, Function<ReactiveConnection, CompletionStage<R>> operation) {
		return reactiveConnection
				.openConnection()
				.thenCompose(operation);
	}

}
