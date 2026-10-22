/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.common;

import java.lang.invoke.MethodHandles;
import java.util.Locale;
import java.util.Objects;

import org.hibernate.reactive.logging.internal.Log;
import org.hibernate.reactive.logging.internal.LoggerFactory;

import io.vertx.core.Context;
import io.vertx.core.internal.ContextInternal;

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

	public static void assertCurrentContextMatches(Object object, ContextInternal expectedContext) {
		if ( ENFORCE ) {
			final ContextInternal currentContext = ContextInternal.current();
			Objects.requireNonNull( currentContext, "Current context cannot be null" );
			if ( !isSameExecutionContext( expectedContext, currentContext ) ) {
				throw LOG.unexpectedContextDetected( object, expectedContext, currentContext );
			}
		}
	}

	/**
	 * Duplicated contexts (of the same root context, or of another duplicate) share the
	 * event loop of their root, and so they are equivalent as far as the thread-safety of a
	 * connection is concerned. Frameworks such as Quarkus legitimately switch to a
	 * different duplicated context (for example, to propagate the context locals to a task
	 * or to a REST client invocation), therefore comparing the instances with {@code equals}
	 * would be too strict.
	 * <p>
	 * Two contexts are considered the same if they have the same root context
	 * or run on the same event loop.
	 */
	private static boolean isSameExecutionContext(ContextInternal expected, ContextInternal current) {
		return current == expected
				|| current.unwrap() == expected.unwrap()
				|| ( current.nettyEventLoop() != null && current.nettyEventLoop() == expected.nettyEventLoop() );
	}
}
