/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.insert;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.id.insert.Binder;
import org.hibernate.reactive.generator.values.ReactiveGeneratedValuesMutationDelegate;

/**
 * @see org.hibernate.id.insert.InsertGeneratedIdentifierDelegate
 */
public interface ReactiveInsertGeneratedIdentifierDelegate extends ReactiveGeneratedValuesMutationDelegate {

	CompletionStage<GeneratedValues> reactivePerformInsertReturning(
			String insertSQL,
			SharedSessionContractImplementor session,
			Binder binder);
}
