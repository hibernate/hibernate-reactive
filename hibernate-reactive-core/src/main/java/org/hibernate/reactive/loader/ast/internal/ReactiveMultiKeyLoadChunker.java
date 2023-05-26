/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.Bindable;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveSelectExecutor;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.internal.JdbcParameterBindingsImpl;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcOperationQuerySelect;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.exec.spi.JdbcParametersList;
import org.hibernate.sql.results.internal.RowTransformerStandardImpl;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.whileLoop;

/**
 * @see org.hibernate.loader.ast.internal.MultiKeyLoadChunker
 */
public class ReactiveMultiKeyLoadChunker<K> {
	@FunctionalInterface
	interface SqlExecutionContextCreator {
		ExecutionContext createContext(
				JdbcParameterBindings parameterBindings,
				SharedSessionContractImplementor session);
	}

	@FunctionalInterface
	interface KeyCollector<K> {
		void collect(K key, int relativePosition, int absolutePosition);
	}

	@FunctionalInterface
	interface ChunkStartListener {
		void chunkStartNotification(int startIndex);
	}

	@FunctionalInterface
	interface ChunkBoundaryListener {
		void chunkBoundaryNotification(int startIndex, int nonNullElementCount);
	}

	private final int chunkSize;
	private final int keyColumnCount;
	private final Bindable bindable;

	private final JdbcParametersList jdbcParameters;
	private final SelectStatement sqlAst;
	private final JdbcOperationQuerySelect jdbcSelect;

	public ReactiveMultiKeyLoadChunker(
			int chunkSize,
			int keyColumnCount,
			Bindable bindable,
			JdbcParametersList jdbcParameters,
			SelectStatement sqlAst,
			JdbcOperationQuerySelect jdbcSelect) {
		this.chunkSize = chunkSize;
		this.keyColumnCount = keyColumnCount;
		this.bindable = bindable;
		this.jdbcParameters = jdbcParameters;
		this.sqlAst = sqlAst;
		this.jdbcSelect = jdbcSelect;
	}

	/**
	 * Process the chunks
	 *
	 * @param keys The group of keys to be initialized
	 * @param nonNullElementCount The number of non-null values in {@code keys}, which will be
	 * 		less-than-or-equal-to the number of {@code keys}
	 * @param startListener Notifications that processing a chunk has starting
	 * @param keyCollector Called for each key as it is processed
	 * @param boundaryListener Notifications that processing a chunk has completed
	 */
	public CompletionStage<Void> processChunks(
			K[] keys,
			int nonNullElementCount,
			ReactiveMultiKeyLoadChunker.SqlExecutionContextCreator sqlExecutionContextCreator,
			ReactiveMultiKeyLoadChunker.KeyCollector<K> keyCollector,
			ReactiveMultiKeyLoadChunker.ChunkStartListener startListener,
			ReactiveMultiKeyLoadChunker.ChunkBoundaryListener boundaryListener,
			SharedSessionContractImplementor session) {
		int[] numberOfKeysLeft = { nonNullElementCount };
		int[] start = { 0 };
		if ( numberOfKeysLeft[0] > 0 ) {
			return whileLoop( () -> processChunk(
									  keys,
									  start[0],
									  sqlExecutionContextCreator,
									  keyCollector,
									  startListener,
									  boundaryListener,
									  session
							  )
					.thenApply( unused -> {
						start[0] += chunkSize;
						numberOfKeysLeft[0] -= chunkSize;
						return numberOfKeysLeft[0] > 0;
					} )
			);
		}
		return voidFuture();
	}

	private CompletionStage<Void> processChunk(
			K[] keys,
			int startIndex,
			ReactiveMultiKeyLoadChunker.SqlExecutionContextCreator sqlExecutionContextCreator,
			ReactiveMultiKeyLoadChunker.KeyCollector<K> keyCollector,
			ReactiveMultiKeyLoadChunker.ChunkStartListener startListener,
			ReactiveMultiKeyLoadChunker.ChunkBoundaryListener boundaryListener,
			SharedSessionContractImplementor session) {
		startListener.chunkStartNotification( startIndex );

		final int parameterCount = chunkSize * keyColumnCount;
		final JdbcParameterBindings jdbcParameterBindings = new JdbcParameterBindingsImpl( parameterCount );

		int nonNullCounter = 0;
		int bindCount = 0;
		for ( int i = 0; i < chunkSize; i++ ) {
			// the position within `K[] keys`
			final int keyPosition = i + startIndex;
			final K value = keyPosition >= keys.length ? null : keys[keyPosition];

			keyCollector.collect( value, i, keyPosition );

			if ( value != null ) {
				nonNullCounter++;
			}

			bindCount += jdbcParameterBindings
					.registerParametersForEachJdbcValue( value, bindCount, bindable, jdbcParameters, session );
		}
		assert bindCount == jdbcParameters.size();

		if ( nonNullCounter == 0 ) {
			// there are no non-null keys in the chunk
			return voidFuture();
		}

		final int finalNonNullCounter = nonNullCounter;
		return StandardReactiveSelectExecutor.INSTANCE.list(
				jdbcSelect,
				jdbcParameterBindings,
				sqlExecutionContextCreator.createContext( jdbcParameterBindings, session ),
				RowTransformerStandardImpl.instance(),
				ReactiveListResultsConsumer.UniqueSemantic.FILTER
		)
				.thenAccept( objects -> boundaryListener.chunkBoundaryNotification( startIndex, finalNonNullCounter ) );
	}
}
