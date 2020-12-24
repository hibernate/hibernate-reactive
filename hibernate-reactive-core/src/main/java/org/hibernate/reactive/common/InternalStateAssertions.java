/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.common;

import io.vertx.core.Context;

/**
 * Commonly used assertions to verify that the operations
 * are running on the expected events and threads.
 * @author Sanne Grinovero
 */
public final class InternalStateAssertions {

	private static final boolean ENFORCE = Boolean.getBoolean( "org.hibernate.reactive.common.InternalStateAssertions.ENFORCE" );

	private InternalStateAssertions() {
		//do not construct
	}

	public static void assertUseOnEventLoop() {
		if ( ENFORCE && (! Context.isOnEventLoopThread() ) ) {
			throw new IllegalStateException( "This method should exclusively be invoked from a Vert.x EventLoop thread; currently running on thread '" + Thread.currentThread().getName() + '\'' );
		}
	}

	public static void assertCurrentThreadMatches(Thread expectedThread) {
		if ( ENFORCE && ( Thread.currentThread() != expectedThread ) ) {
			throw new IllegalStateException( "Detected use of the Reactive Session from a different Thread than the one which was used to open the Reactive Session - this suggests an invalid integration; "
			+ "original thread: '" + expectedThread.getName() + "' current Thread: '" + Thread.currentThread().getName() + '\'' );
		}
	}

}
