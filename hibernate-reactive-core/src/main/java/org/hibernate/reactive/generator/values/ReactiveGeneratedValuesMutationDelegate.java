/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.generator.values;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.jdbc.mutation.JdbcValueBindings;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.generator.values.GeneratedValuesMutationDelegate;

public interface ReactiveGeneratedValuesMutationDelegate extends GeneratedValuesMutationDelegate {

	CompletionStage<GeneratedValues> reactivePerformMutation(
			PreparedStatementDetails singleStatementDetails,
			JdbcValueBindings jdbcValueBindings,
			Object modelReference,
			SharedSessionContractImplementor session);
}
