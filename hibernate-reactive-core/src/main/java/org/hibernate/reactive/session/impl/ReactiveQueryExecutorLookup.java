/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.session.ReactiveQueryExecutor;

/**
 * This is a dirty trick to mitigate the performance impact of JDK-8180450;
 * hopefully temporary but we have no indication about a possible fix at
 * the moment.
 * The gist is that we need to avoid repeatedly checking for ReactiveSessionImpl
 * to implement certain interfaces; we frequently need to cast the current
 * {@see SharedSessionContractImplementor} to {@see ReactiveQueryExecutor}:
 * let's take advantage of the fact that it's almost certainly going to be
 * of a known concrete type, specifically a {@see ReactiveSessionImpl}.
 * @link https://bugs.openjdk.org/browse/JDK-8180450
 */
public final class ReactiveQueryExecutorLookup {

	/**
	 * Extracts the ReactiveQueryExecutor from a Session.
	 * @param session
	 * @return
	 */
	public static ReactiveQueryExecutor extract(final SharedSessionContractImplementor session) {
		if ( session instanceof org.hibernate.reactive.session.impl.ReactiveSessionImpl ) {
			return (org.hibernate.reactive.session.impl.ReactiveSessionImpl) session;
		}
		else {
			return (ReactiveQueryExecutor) session;
		}
	}

}
