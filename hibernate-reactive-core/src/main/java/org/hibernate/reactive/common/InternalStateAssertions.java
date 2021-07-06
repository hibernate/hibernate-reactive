/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.common;

import java.util.Locale;

import io.vertx.core.Context;

/**
 * Commonly used assertions to verify that the operations
 * are running on the expected events and threads.
 * @author Sanne Grinovero
 */
public final class InternalStateAssertions {

	/**
	 * Unless this system property is explicitly set to "false", we will enforce these checks.
	 */
	private static final boolean ENFORCE = ! "false".equalsIgnoreCase(
			System.getProperty( "org.hibernate.reactive.common.InternalStateAssertions.ENFORCE", "true" )
			.toLowerCase( Locale.ROOT )
			.trim()
	);

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
			throw new IllegalStateException( "Detected use of the reactive Session from a different Thread than the one which was used to open the reactive Session - this suggests an invalid integration; "
			+ "original thread: '" + expectedThread.getName() + "' current Thread: '" + Thread.currentThread().getName() + '\'' );
		}
	}

}
