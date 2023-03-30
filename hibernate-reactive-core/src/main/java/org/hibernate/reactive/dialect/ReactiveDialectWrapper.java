/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.dialect;

import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.DialectDelegateWrapper;
import org.hibernate.dialect.identity.IdentityColumnSupport;
import org.hibernate.reactive.dialect.identity.ReactiveIdentityColumnSupportAdapter;

/**
 * Wraps the given dialect to make some internal components reactive;
 * also, potentially applies a SQL syntax workaround if the wrapped Dialect
 * is extending PostgreSQLDialect.
 */
public final class ReactiveDialectWrapper extends DialectDelegateWrapper {

	/**
	 * A utility method to help checking the actual dialect class.
	 * @return true, if the dialect is an instance of one of the classes
	 */
	public static boolean instanceOf(Dialect dialect, Class<?>... dialectClasses) {
		Dialect realDialect = extractRealDialect( dialect );
		for ( Class<?> dialectClass : dialectClasses ) {
			if ( dialectClass.isInstance( realDialect ) ) {
				return true;
			}
		}
		return false;
	}

	public ReactiveDialectWrapper(Dialect wrapped) {
		super( wrapped );
	}

	@Override
	public IdentityColumnSupport getIdentityColumnSupport() {
		return new ReactiveIdentityColumnSupportAdapter( super.getIdentityColumnSupport() );
	}
}
