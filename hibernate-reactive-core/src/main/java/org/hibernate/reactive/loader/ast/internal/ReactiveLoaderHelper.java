/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.lang.reflect.Array;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.LockOptions;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.SubselectFetch;
import org.hibernate.metamodel.mapping.JdbcMapping;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveSelectExecutor;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.internal.JdbcParameterBindingImpl;
import org.hibernate.sql.exec.internal.JdbcParameterBindingsImpl;
import org.hibernate.sql.exec.spi.JdbcOperationQuerySelect;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.results.internal.RowTransformerStandardImpl;

import static java.util.Objects.requireNonNull;

/**
 * @see org.hibernate.loader.ast.internal.LoaderHelper
 */
public class ReactiveLoaderHelper {

	/**
	 * Creates a typed array, as opposed to a generic {@code Object[]} that holds the typed values
	 *
	 * @param elementClass The type of the array elements.  See {@link Class#getComponentType()}
	 * @param length The length to which the array should be created.  This is usually zero for Hibernate uses
	 */
	public static <X> X[] createTypedArray(Class<X> elementClass, @SuppressWarnings("SameParameterValue") int length) {
		//noinspection unchecked
		return (X[]) Array.newInstance( elementClass, length );
	}

	/**
	 * Load one or more instances of a model part (an entity or collection)
	 * based on a SQL ARRAY parameter to specify the keys (as opposed to the
	 * more traditional SQL IN predicate approach).
	 *
	 * @param <R> The type of the model part to load
	 * @param <K> The type of the keys
	 */
	public static <R, K> CompletionStage<List<R>> loadByArrayParameter(
			K[] idsToInitialize,
			SelectStatement sqlAst,
			JdbcOperationQuerySelect jdbcOperation,
			JdbcParameter jdbcParameter,
			JdbcMapping arrayJdbcMapping,
			Object entityId,
			Object entityInstance,
			LockOptions lockOptions,
			Boolean readOnly,
			SharedSessionContractImplementor session) {
		requireNonNull( jdbcOperation );
		requireNonNull( jdbcParameter );

		final JdbcParameterBindings jdbcParameterBindings = new JdbcParameterBindingsImpl( 1 );
		jdbcParameterBindings.addBinding(
				jdbcParameter,
				new JdbcParameterBindingImpl( arrayJdbcMapping, idsToInitialize )
		);

		final SubselectFetch.RegistrationHandler subSelectFetchableKeysHandler = SubselectFetch.createRegistrationHandler(
				session.getPersistenceContext().getBatchFetchQueue(),
				sqlAst,
				Collections.singletonList( jdbcParameter ),
				jdbcParameterBindings
		);

		return StandardReactiveSelectExecutor.INSTANCE.list(
				jdbcOperation,
				jdbcParameterBindings,
				new SingleIdExecutionContext(
						entityId,
						entityInstance,
						readOnly,
						lockOptions,
						subSelectFetchableKeysHandler,
						session
				),
				RowTransformerStandardImpl.instance(),
				ReactiveListResultsConsumer.UniqueSemantic.FILTER
		);
	}
}
