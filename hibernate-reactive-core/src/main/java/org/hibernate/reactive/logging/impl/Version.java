/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.logging.impl;

/**
 * Information about the version of Hibernate Reactive.
 *
 * @author Steve Ebersole
 */
public final class Version {

	private static final String VERSION;
	static {
		final String version = Version.class.getPackage().getImplementationVersion();
		VERSION = version != null ? version : "[WORKING]";
	}

	private Version() {
	}

	/**
	 * Access to the Hibernate Reactive version.
	 *
	 * @return The version
	 */
	public static String getVersionString() {
		return VERSION;
	}
}
