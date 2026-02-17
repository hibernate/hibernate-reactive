/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.ast.internal.LoaderSqlAstCreationState;
import org.hibernate.loader.ast.internal.SimpleNaturalIdLoader;
import org.hibernate.loader.ast.spi.NaturalIdLoadOptions;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.internal.SimpleNaturalIdMapping;
import org.hibernate.reactive.loader.ast.spi.ReactiveNaturalIdLoader;
import org.hibernate.sql.ast.tree.from.TableGroup;

public class ReactiveSimpleNaturalIdLoader<T> extends SimpleNaturalIdLoader<CompletionStage<T>>
		implements ReactiveNaturalIdLoader<T> {

	private final ReactiveNaturalIdLoaderDelegate<T> delegate;

	public ReactiveSimpleNaturalIdLoader(SimpleNaturalIdMapping naturalIdMapping, EntityMappingType entityDescriptor) {
		super( naturalIdMapping, entityDescriptor );
		delegate = new ReactiveNaturalIdLoaderDelegate<>( naturalIdMapping, entityDescriptor ) {
			@Override
			protected void applyNaturalIdRestriction(
					Object bindValue,
					TableGroup rootTableGroup,
					Consumer consumer,
					BiConsumer jdbcParameterConsumer,
					LoaderSqlAstCreationState sqlAstCreationState,
					SharedSessionContractImplementor session) {
				ReactiveSimpleNaturalIdLoader.this.applyNaturalIdRestriction(
						bindValue,
						rootTableGroup,
						consumer,
						jdbcParameterConsumer,
						sqlAstCreationState,
						session
				);
			}
		};
	}

	/**
	 * @see org.hibernate.loader.ast.internal.AbstractNaturalIdLoader#resolveIdToNaturalId(Object, SharedSessionContractImplementor)
	 */
	@Override
	public CompletionStage<Object> resolveIdToNaturalId(Object id, SharedSessionContractImplementor session) {
		return delegate.resolveIdToNaturalId( id, session );
	}

	@Override
	public CompletionStage<Object> resolveNaturalIdToId(
			Object naturalIdValue,
			SharedSessionContractImplementor session) {
		return delegate.resolveNaturalIdToId( naturalIdValue, session );
	}

	@Override
	public CompletionStage<T> load(
			Object naturalIdValue,
			NaturalIdLoadOptions options,
			SharedSessionContractImplementor session) {
		return delegate.load( naturalIdValue, options, session );
	}
}
