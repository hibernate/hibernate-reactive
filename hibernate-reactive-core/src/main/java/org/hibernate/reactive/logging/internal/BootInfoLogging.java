/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.logging.internal;

import java.lang.invoke.MethodHandles;

/**
 * Centralized logging for details we log as {@code INFO} on bootstrap, such as
 * the version of Hibernate Reactive and the configuration of the connection pool,
 * which certain environments routinely want to suppress.
 * <p>
 * Using the dedicated category {@value #CATEGORY_NAME} allows that, in the same way
 * as the category {@code org.hibernate.orm.info} does for Hibernate ORM.
 *
 * @see org.hibernate.boot.internal.BootInfoLogging
 */
public final class BootInfoLogging {

	public static final String CATEGORY_NAME = "org.hibernate.reactive.info";

	public static final Log BOOT_INFO_LOGGER = LoggerFactory
			.make( Log.class, new LogCategory( CATEGORY_NAME ), MethodHandles.lookup() );

	private BootInfoLogging() {
	}

	/**
	 * Logs the {@code HR000001: Hibernate Reactive version {version}} message
	 * using the {@value #CATEGORY_NAME} category name.
	 *
	 * @see Log#startHibernateReactive(String)
	 */
	public static void logVersion() {
		BOOT_INFO_LOGGER.startHibernateReactive( Version.getVersionString() );
	}
}
