/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.insert;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.insert.Binder;

/**
 * @see org.hibernate.id.insert.InsertGeneratedIdentifierDelegate
 */
public interface ReactiveInsertGeneratedIdentifierDelegate {

	CompletionStage<Object> reactivePerformInsert(
			PreparedStatementDetails insertStatementDetails,
			JdbcValueBindings valueBindings,
			Object entity,
			SharedSessionContractImplementor session);

	CompletionStage<Object> reactivePerformInsert(String insertSQL, SharedSessionContractImplementor session, Binder binder);
}
