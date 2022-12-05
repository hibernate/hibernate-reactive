/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc.mutation.internal;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.NotYetImplementedFor6Exception;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.mutation.internal.JdbcValueBindingsImpl;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.sql.model.MutationTarget;
import org.hibernate.sql.model.MutationType;

/**
 * @see org.hibernate.engine.jdbc.mutation.JdbcValueBindings
 */
public class ReactiveValueBindings extends JdbcValueBindingsImpl {

	Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveValueBindings(MutationType mutationType, MutationTarget<?> mutationTarget, JdbcValueDescriptorAccess jdbcValueDescriptorAccess) {
		super( mutationType, mutationTarget, jdbcValueDescriptorAccess );
	}

	@Override
	public void beforeStatement(PreparedStatementDetails statementDetails, SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "beforeReactiveStatement" );
	}

	public CompletionStage<Void> beforeReactiveStatement(PreparedStatementDetails statementDetails, SharedSessionContractImplementor session) {
		throw new NotYetImplementedFor6Exception();
	}
}
