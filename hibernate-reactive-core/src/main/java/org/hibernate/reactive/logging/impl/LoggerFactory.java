/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.logging.impl;

import java.lang.invoke.MethodHandles;
import java.util.Locale;

import org.jboss.logging.Logger;

public final class LoggerFactory {

	private LoggerFactory() {
		// Not allowed
	}

	// NOTE: Locale.ROOT is passed intentionally.
	//
	// If localized log messages are introduced in the future, this must be
	// revisited and the explicit ROOT locale removed.

	public static <T> T make(Class<T> logClass, MethodHandles.Lookup creationContext) {
		final String className = creationContext.lookupClass().getName();
		return Logger.getMessageLogger( creationContext, logClass, className, Locale.ROOT );
	}

	public static <T> T make(Class<T> logClass, LogCategory category, MethodHandles.Lookup creationContext) {
		return Logger.getMessageLogger( creationContext, logClass, category.getName(), Locale.ROOT );
	}
}
