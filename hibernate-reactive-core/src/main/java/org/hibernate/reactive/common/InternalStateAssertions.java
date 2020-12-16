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

	private InternalStateAssertions() {
		//do not construct
	}

	public static void assertUseOnEventLoop() {
		assert Context.isOnEventLoopThread() : "This method should exclusively be invoked from a Vert.x EventLoop thread; running on '" + Thread.currentThread().getName() + "'";
	}

}
