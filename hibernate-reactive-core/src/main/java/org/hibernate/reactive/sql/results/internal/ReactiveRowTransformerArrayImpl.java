/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.internal;

import java.util.concurrent.CompletionStage;

import org.hibernate.sql.results.internal.RowTransformerArrayImpl;
import org.hibernate.sql.results.spi.RowTransformer;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

/**
 * @see org.hibernate.sql.results.internal.RowTransformerArrayImpl
 */
public class ReactiveRowTransformerArrayImpl implements RowTransformer<CompletionStage<Object[]>> {

	/**
	 * Singleton access
	 *
	 * @see #instance()
	 */
	private static final RowTransformerArrayImpl INSTANCE = new RowTransformerArrayImpl();

	public static RowTransformerArrayImpl instance() {
		return INSTANCE;
	}

	// I'm not sure why I need this
	public static RowTransformer<CompletionStage<Object[]>> asRowTransformer() {
		return (RowTransformer) INSTANCE;
	}

	@Override
	public CompletionStage<Object[]> transformRow(Object[] objects) {
		return completedFuture( objects );
	}
}
