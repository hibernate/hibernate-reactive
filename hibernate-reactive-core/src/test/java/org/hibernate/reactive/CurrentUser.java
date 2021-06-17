/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import org.hibernate.Session;
import org.hibernate.tuple.ValueGenerator;

/**
 * An example from the Hibernate ORM documentation that we use for testing of
 * entities using @{@link org.hibernate.annotations.GeneratorType}
 *
 * @see GeneratedPropertyJoinedTableTest
 * @see GeneratedPropertySingleTableTest
 * @see GeneratedPropertyUnionSubclassesTest
 */
public class CurrentUser {

	public static final CurrentUser INSTANCE = new CurrentUser();

	private static final ThreadLocal<String> storage = new ThreadLocal<>();

	public void logIn(String user) {
		storage.set( user );
	}

	public void logOut() {
		storage.remove();
	}

	public String get() {
		return storage.get();
	}

	public static class LoggedUserGenerator implements ValueGenerator<String> {

		@Override
		public String generateValue(Session session, Object owner) {
			return CurrentUser.INSTANCE.get();
		}
	}
}
