/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.common;

import java.lang.invoke.MethodHandles;
import java.util.Locale;

import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;

import io.vertx.core.Context;

/**
 * Commonly used assertions to verify that the operations
 * are running on the expected events and threads.
 * @author Sanne Grinovero
 */
public final class InternalStateAssertions {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

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
			throw LOG.shouldBeInvokedInVertxEventLoopThread( Thread.currentThread().getName() );
		}
	}

	public static void assertCurrentThreadMatches(Thread expectedThread) {
		if ( ENFORCE && ( Thread.currentThread() != expectedThread ) ) {
			throw LOG.detectedUsedOfTheSessionOnTheWrongThread(
					expectedThread.getId(),
					expectedThread.getName(),
					Thread.currentThread().getId(),
					Thread.currentThread().getName()
			);
		}
	}

}
