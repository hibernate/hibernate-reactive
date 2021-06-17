/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.engine.spi.SharedSessionContractImplementor;

import java.io.Serializable;

public class SessionUtil {

	public static void throwEntityNotFound(SharedSessionContractImplementor session, String entityName, Serializable identifier) {
		session.getFactory().getEntityNotFoundDelegate().handleEntityNotFound( entityName, identifier );
	}

	public static void checkEntityFound(SharedSessionContractImplementor session, String entityName, Serializable identifier, Object optional) {
		if ( optional==null ) {
			throwEntityNotFound(session, entityName, identifier);
		}
	}

}
