/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.insert;

import org.hibernate.reactive.engine.impl.InternalStage;

import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.insert.Binder;

/**
 * @see org.hibernate.id.insert.InsertGeneratedIdentifierDelegate
 */
public interface ReactiveInsertGeneratedIdentifierDelegate {

	InternalStage<Object> reactivePerformInsert(
			PreparedStatementDetails insertStatementDetails,
			JdbcValueBindings valueBindings,
			Object entity,
			SharedSessionContractImplementor session);

	InternalStage<Object> reactivePerformInsert(String insertSQL, SharedSessionContractImplementor session, Binder binder);
}
