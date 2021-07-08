/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.logging.impl;

import java.lang.invoke.MethodHandles;

import org.jboss.logging.Logger;

public final class LoggerFactory {

	private LoggerFactory() {
		// Not allowed
	}

	public static <T> T make(Class<T> logClass, MethodHandles.Lookup creationContext) {
		final String className = creationContext.lookupClass().getName();
		return Logger.getMessageLogger( logClass, className );
	}

	public static <T> T make(Class<T> logClass, LogCategory category) {
		return Logger.getMessageLogger( logClass, category.getName() );
	}
}
