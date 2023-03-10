/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.internal.util.collections.ArrayHelper;
import org.hibernate.query.named.RowReaderMemento;
import org.hibernate.reactive.sql.exec.spi.ReactiveRowProcessingState;
import org.hibernate.reactive.sql.results.graph.ReactiveDomainResultsAssembler;
import org.hibernate.reactive.sql.results.spi.ReactiveRowReader;
import org.hibernate.sql.results.graph.DomainResultAssembler;
import org.hibernate.sql.results.graph.Initializer;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesSourceProcessingOptions;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesSourceProcessingState;
import org.hibernate.sql.results.jdbc.spi.RowProcessingState;
import org.hibernate.sql.results.spi.RowTransformer;
import org.hibernate.type.descriptor.java.JavaType;

import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;
import static org.hibernate.sql.results.LoadingLogger.LOGGER;


/**
 * @see org.hibernate.sql.results.internal.StandardRowReader
 */
public class ReactiveStandardRowReader<R> implements ReactiveRowReader<R> {

	private final List<DomainResultAssembler<?>> resultAssemblers;
	private final ReactiveInitializersList initializers;
	private final RowTransformer<R> rowTransformer;
	private final Class<R> domainResultJavaType;

	private final int assemblerCount;

	public ReactiveStandardRowReader(
			List<DomainResultAssembler<?>> resultAssemblers,
			ReactiveInitializersList initializers,
			RowTransformer<R> rowTransformer,
			Class<R> domainResultJavaType) {
		this.resultAssemblers = resultAssemblers;
		this.initializers = initializers;
		this.rowTransformer = rowTransformer;
		this.assemblerCount = resultAssemblers.size();
		this.domainResultJavaType = domainResultJavaType;
	}

	@Override
	public CompletionStage<R> reactiveReadRow(ReactiveRowProcessingState rowProcessingState, JdbcValuesSourceProcessingOptions options) {
		LOGGER.trace( "ReactiveStandardRowReader#readRow" );

		return coordinateInitializers( rowProcessingState )
				.thenCompose( v -> {
					final Object[] resultRow = new Object[assemblerCount];
					return loop( 0, assemblerCount, i -> {
						final DomainResultAssembler assembler = resultAssemblers.get( i );
						LOGGER.debugf( "Calling top-level assembler (%s / %s) : %s", i, assemblerCount, assembler );
						if ( assembler instanceof ReactiveDomainResultsAssembler ) {
							return ( (ReactiveDomainResultsAssembler) assembler )
									.reactiveAssemble( rowProcessingState, options )
									.thenAccept( obj -> resultRow[i] = obj );
						}
						resultRow[i] = assembler.assemble( rowProcessingState, options );
						return voidFuture();
					} )
					.thenApply( ignore -> {
						afterRow( rowProcessingState );
						return rowTransformer.transformRow( resultRow );
					} );
				} );
	}

	@Override
	public Class<R> getDomainResultResultJavaType() {
		return domainResultJavaType;
	}

	@Override
	public Class<?> getResultJavaType() {
		if ( resultAssemblers.size() == 1 ) {
			return resultAssemblers.get( 0 ).getAssembledJavaType().getJavaTypeClass();
		}

		return Object[].class;
	}

	@Override
	public List<JavaType<?>> getResultJavaTypes() {
		List<JavaType<?>> javaTypes = new ArrayList<>( resultAssemblers.size() );
		for ( DomainResultAssembler resultAssembler : resultAssemblers ) {
			javaTypes.add( resultAssembler.getAssembledJavaType() );
		}
		return javaTypes;
	}

	@Override
	public List<Initializer> getInitializers() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ReactiveInitializersList getReactiveInitializersList() {
		return initializers;
	}

	@Override
	public R readRow(RowProcessingState rowProcessingState, JdbcValuesSourceProcessingOptions options) {
		throw LOG.nonReactiveMethodCall( "reactiveRowReader" );
	}

	private void afterRow(RowProcessingState rowProcessingState) {
		LOGGER.trace( "ReactiveStandardRowReader#afterRow" );
		initializers.finishUpRow( rowProcessingState );
	}

	private CompletionStage<Void> coordinateInitializers(ReactiveRowProcessingState rowProcessingState) {
		initializers.resolveKeys( rowProcessingState );
		return initializers.resolveInstances( rowProcessingState )
				.thenCompose( v -> initializers.initializeInstance( rowProcessingState ) );
	}

	@Override
	@SuppressWarnings("ForLoopReplaceableByForEach")
	public void finishUp(JdbcValuesSourceProcessingState processingState) {
		initializers.endLoading( processingState.getExecutionContext() );
	}

	@Override
	public RowReaderMemento toMemento(SessionFactoryImplementor factory) {
		return new RowReaderMemento() {
			@Override
			public Class<?>[] getResultClasses() {
				return ArrayHelper.EMPTY_CLASS_ARRAY;
			}

			@Override
			public String[] getResultMappingNames() {
				return ArrayHelper.EMPTY_STRING_ARRAY;
			}
		};
	}
}
