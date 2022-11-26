/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.internal;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.sql.results.jdbc.internal.DirectResultSetAccess;

public class ReactiveDirectResultSetAccess extends DirectResultSetAccess {

	public ReactiveDirectResultSetAccess(SharedSessionContractImplementor persistenceContext, PreparedStatement resultSetSource, ResultSet resultSet) {
		super( persistenceContext, resultSetSource, resultSet );
	}
}
