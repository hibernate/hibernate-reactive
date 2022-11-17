/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.spi;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.ResultListTransformer;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.reactive.sql.exec.spi.ReactiveRowProcessingState;
import org.hibernate.reactive.sql.exec.spi.ReactiveValuesResultSet;
import org.hibernate.sql.results.jdbc.internal.JdbcValuesSourceProcessingStateStandardImpl;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesSourceProcessingOptions;
import org.hibernate.sql.results.spi.RowReader;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.java.spi.EntityJavaType;
import org.hibernate.type.descriptor.java.spi.JavaTypeRegistry;
import org.hibernate.type.spi.TypeConfiguration;

import static org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer.UniqueSemantic.ALLOW;
import static org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer.UniqueSemantic.ASSERT;
import static org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer.UniqueSemantic.FILTER;
import static org.hibernate.reactive.util.impl.CompletionStages.falseFuture;

/**
 *
 * @see org.hibernate.sql.results.spi.ListResultsConsumer
 */
public class ReactiveListResultsConsumer<R> implements ReactiveResultsConsumer<List<R>, R> {

	private static final ReactiveListResultsConsumer<?> NEVER_DE_DUP_CONSUMER = new ReactiveListResultsConsumer<>( ReactiveListResultsConsumer.UniqueSemantic.NEVER );
	private static final ReactiveListResultsConsumer<?> ALLOW_DE_DUP_CONSUMER = new ReactiveListResultsConsumer<>( ALLOW );
	private static final ReactiveListResultsConsumer<?> IGNORE_DUP_CONSUMER = new ReactiveListResultsConsumer<>( ReactiveListResultsConsumer.UniqueSemantic.NONE );
	private static final ReactiveListResultsConsumer<?> DE_DUP_CONSUMER = new ReactiveListResultsConsumer<>( FILTER );
	private static final ReactiveListResultsConsumer<?> ERROR_DUP_CONSUMER = new ReactiveListResultsConsumer<>( ASSERT );

	@Override
	public CompletionStage<List<R>> consume(
			ReactiveValuesResultSet jdbcValues,
			SharedSessionContractImplementor session,
			JdbcValuesSourceProcessingOptions processingOptions,
			JdbcValuesSourceProcessingStateStandardImpl jdbcValuesSourceProcessingState,
			ReactiveRowProcessingState rowProcessingState,
			RowReader<R> rowReader) {
		final PersistenceContext persistenceContext = session.getPersistenceContext();
		final TypeConfiguration typeConfiguration = session.getTypeConfiguration();
		final QueryOptions queryOptions = rowProcessingState.getQueryOptions();

		persistenceContext.getLoadContexts().register( jdbcValuesSourceProcessingState );

		final JavaType<R> domainResultJavaType = resolveDomainResultJavaType(
				rowReader.getDomainResultResultJavaType(),
				rowReader.getResultJavaTypes(),
				typeConfiguration
		);

		final boolean isEntityResultType = domainResultJavaType instanceof EntityJavaType;
		final ReactiveListResultsConsumer.Results<R> results =
				( uniqueSemantic == ALLOW || uniqueSemantic == FILTER ) && isEntityResultType
						? new EntityResult<>( domainResultJavaType )
						: new Results<>( domainResultJavaType );

		Runnable addResultFunction = getAddResultFunction( results, rowReader, rowProcessingState, processingOptions, isEntityResultType );

		return nextState( rowProcessingState, addResultFunction )
				.thenApply( v -> end( results, jdbcValuesSourceProcessingState, persistenceContext, queryOptions ) )
				.handle( (list, ex) -> {
					finish( jdbcValues, session, jdbcValuesSourceProcessingState, rowReader, persistenceContext, ex );
					return list;
				} ) ;
	}

	private void finish(
			ReactiveValuesResultSet jdbcValues,
			SharedSessionContractImplementor session,
			JdbcValuesSourceProcessingStateStandardImpl jdbcValuesSourceProcessingState,
			RowReader<R> rowReader,
			PersistenceContext persistenceContext,
			Throwable ex) {
		try {
			rowReader.finishUp( jdbcValuesSourceProcessingState );
			jdbcValues.finishUp( session );
			persistenceContext.initializeNonLazyCollections();
		}
		catch (RuntimeException e) {
			if ( ex != null ) {
				ex.addSuppressed( e );
			}
			else {
				ex = e;
			}
		}
		finally {
			if ( ex != null ) {
				throw new RuntimeException( ex );
			}
		}
	}

	private CompletionStage<Boolean> nextState(ReactiveRowProcessingState rowProcessingState, Runnable addResultFunction) {
		return rowProcessingState.next()
				.thenCompose( hasNext -> {
					if ( hasNext ) {
						addResultFunction.run();
						rowProcessingState.finishRowProcessing();
						return nextState( rowProcessingState, addResultFunction );
					}
					return falseFuture();
				} );
	}

	private Runnable getAddResultFunction(
			ReactiveListResultsConsumer.Results<R> results,
			RowReader<R> rowReader,
			ReactiveRowProcessingState rowProcessingState,
			JdbcValuesSourceProcessingOptions processingOptions,
			boolean isEntityResultType) {
		if ( this.uniqueSemantic == FILTER
				|| this.uniqueSemantic == ASSERT && rowProcessingState.hasCollectionInitializers
				|| this.uniqueSemantic == ALLOW && isEntityResultType ) {
			return () -> results.addUnique( rowReader.readRow( rowProcessingState, processingOptions ) );
		}

		if ( this.uniqueSemantic == ASSERT ) {
			return () -> {
				R row = rowReader.readRow( rowProcessingState, processingOptions );
				boolean unique = results.addUnique( row );
				if ( !unique ) {
					throw new HibernateException( String.format( Locale.ROOT, "Duplicate row was found and `%s` was specified", ASSERT ) );
				}
			};
		}

		return () -> results.add( rowReader.readRow( rowProcessingState, processingOptions ) );
	}

	private List<R> end(
			ReactiveListResultsConsumer.Results<R> results,
			JdbcValuesSourceProcessingStateStandardImpl jdbcValuesSourceProcessingState,
			PersistenceContext persistenceContext,
			QueryOptions queryOptions) {
		try {
			jdbcValuesSourceProcessingState.finishUp();
		}
		finally {
			persistenceContext.getLoadContexts().deregister( jdbcValuesSourceProcessingState );
		}

		final ResultListTransformer<R> resultListTransformer = (ResultListTransformer<R>) queryOptions
				.getResultListTransformer();
		return resultListTransformer != null
				? resultListTransformer.transformList( results.getResults() )
				: results.getResults();
	}

	@SuppressWarnings("unchecked")
	public static <R> ReactiveListResultsConsumer<R> instance(ReactiveListResultsConsumer.UniqueSemantic uniqueSemantic) {
		switch ( uniqueSemantic ) {
			case ASSERT: {
				return (ReactiveListResultsConsumer<R>) ERROR_DUP_CONSUMER;
			}
			case FILTER: {
				return (ReactiveListResultsConsumer<R>) DE_DUP_CONSUMER;
			}
			case NEVER: {
				return (ReactiveListResultsConsumer<R>) NEVER_DE_DUP_CONSUMER;
			}
			case ALLOW: {
				return (ReactiveListResultsConsumer<R>) ALLOW_DE_DUP_CONSUMER;
			}
			default: {
				return (ReactiveListResultsConsumer<R>) IGNORE_DUP_CONSUMER;
			}
		}
	}

	/**
	 * Ways this consumer can handle in-memory row de-duplication
	 */
	public enum UniqueSemantic {
		/**
		 * Apply no in-memory de-duplication
		 */
		NONE,

		/**
		 * Apply in-memory de-duplication, removing rows already part of the results
		 */
		FILTER,

		/**
		 * Apply in-memory duplication checks, throwing a HibernateException when duplicates are found
		 */
		ASSERT,

		/**
		 * Never apply unique handling.  E.g. for NativeQuery.  Whereas {@link #NONE} can be adjusted,
		 * NEVER will never apply unique handling
		 */
		NEVER,

		/**
		 * De-duplication is allowed if the query and result type allow
		 */
		ALLOW
	}

	private final ReactiveListResultsConsumer.UniqueSemantic uniqueSemantic;

	public ReactiveListResultsConsumer(ReactiveListResultsConsumer.UniqueSemantic uniqueSemantic) {
		this.uniqueSemantic = uniqueSemantic;
	}

	private JavaType<R> resolveDomainResultJavaType(
			Class<R> domainResultResultJavaType,
			List<JavaType<?>> resultJavaTypes,
			TypeConfiguration typeConfiguration) {
		final JavaTypeRegistry javaTypeRegistry = typeConfiguration.getJavaTypeRegistry();

		if ( domainResultResultJavaType != null ) {
			return javaTypeRegistry.resolveDescriptor( domainResultResultJavaType );
		}

		if ( resultJavaTypes.size() == 1 ) {
			//noinspection unchecked
			return (JavaType<R>) resultJavaTypes.get( 0 );
		}

		return javaTypeRegistry.resolveDescriptor( Object[].class );
	}

	@Override
	public boolean canResultsBeCached() {
		return true;
	}

	@Override
	public String toString() {
		return ReactiveResultsConsumer.class.getSimpleName() + "(" + uniqueSemantic + ")";
	}

	private static class Results<R> {
		private final List<R> results = new ArrayList<>();
		private final JavaType resultJavaType;

		public Results(JavaType resultJavaType) {
			this.resultJavaType = resultJavaType;
		}

		public boolean addUnique(R result) {
			for ( int i = 0; i < results.size(); i++ ) {
				if ( resultJavaType.areEqual( results.get( i ), result ) ) {
					return false;
				}
			}
			results.add( result );
			return true;
		}

		public void add(R result) {
			results.add( result );
		}

		public List<R> getResults() {
			return results;
		}
	}

	private static class EntityResult<R> extends Results<R> {
		private static final Object DUMP_VALUE = new Object();

		private final IdentityHashMap<R, Object> added = new IdentityHashMap<>();

		public EntityResult(JavaType resultJavaType) {
			super( resultJavaType );
		}

		public boolean addUnique(R result) {
			if ( added.put( result, DUMP_VALUE ) == null ) {
				super.add( result );
				return true;
			}
			return false;
		}
	}
}
