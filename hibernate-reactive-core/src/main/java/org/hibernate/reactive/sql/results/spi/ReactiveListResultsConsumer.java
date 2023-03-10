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
import java.util.function.Supplier;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.ResultListTransformer;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.reactive.sql.exec.spi.ReactiveRowProcessingState;
import org.hibernate.reactive.sql.exec.spi.ReactiveValuesResultSet;
import org.hibernate.sql.results.jdbc.internal.JdbcValuesSourceProcessingStateStandardImpl;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesSourceProcessingOptions;
import org.hibernate.sql.results.spi.LoadContexts;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.java.spi.EntityJavaType;
import org.hibernate.type.descriptor.java.spi.JavaTypeRegistry;
import org.hibernate.type.spi.TypeConfiguration;

import static org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer.UniqueSemantic.ALLOW;
import static org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer.UniqueSemantic.ASSERT;
import static org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer.UniqueSemantic.FILTER;
import static org.hibernate.reactive.util.impl.CompletionStages.falseFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.whileLoop;

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

	private static void validateUniqueResult(Boolean unique) {
		if ( !unique ) {
			throw new HibernateException( String.format(
					Locale.ROOT,
					"Duplicate row was found and `%s` was specified",
					ASSERT
			) );
		}
	}

	private static class RegistrationHandler {

		private final LoadContexts contexts;
		private final JdbcValuesSourceProcessingStateStandardImpl state;

		private RegistrationHandler(LoadContexts contexts, JdbcValuesSourceProcessingStateStandardImpl state) {
			this.contexts = contexts;
			this.state = state;
		}

		public void register() {
			contexts.register( state );
		}

		public void deregister() {
			contexts.deregister( state );
		}
	}

	@Override
	public CompletionStage<List<R>> consume(
			ReactiveValuesResultSet jdbcValues,
			SharedSessionContractImplementor session,
			JdbcValuesSourceProcessingOptions processingOptions,
			JdbcValuesSourceProcessingStateStandardImpl jdbcValuesSourceProcessingState,
			ReactiveRowProcessingState rowProcessingState,
			ReactiveRowReader<R> rowReader) {
		final PersistenceContext persistenceContext = session.getPersistenceContext();
		final TypeConfiguration typeConfiguration = session.getTypeConfiguration();
		final QueryOptions queryOptions = rowProcessingState.getQueryOptions();

		persistenceContext.beforeLoad();
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

		Supplier<CompletionStage<Void>> addToResultsSupplier = addToResultsSupplier( results, rowReader, rowProcessingState, processingOptions, isEntityResultType );
		return whileLoop( () -> rowProcessingState.next()
					.thenCompose( hasNext -> {
						if ( hasNext ) {
							return addToResultsSupplier.get()
									.thenApply( unused -> {
										rowProcessingState.finishRowProcessing();
										return true;
									} );
						}
						return falseFuture();
					} )
		)
		.thenApply( v -> finishUp( results, jdbcValuesSourceProcessingState, rowReader, persistenceContext, queryOptions ) )
		.handle( (list, ex) -> {
			end( jdbcValues, session, jdbcValuesSourceProcessingState, rowReader, persistenceContext, ex );
			return list;
		} );
	}

	private Supplier<CompletionStage<Void>> addToResultsSupplier(
			ReactiveListResultsConsumer.Results<R> results,
			ReactiveRowReader<R> rowReader,
			ReactiveRowProcessingState rowProcessingState,
			JdbcValuesSourceProcessingOptions processingOptions,
			boolean isEntityResultType) {
		if ( this.uniqueSemantic == FILTER
				|| this.uniqueSemantic == ASSERT && rowProcessingState.hasCollectionInitializers()
				|| this.uniqueSemantic == ALLOW && isEntityResultType ) {
			return () -> rowReader
					.reactiveReadRow( rowProcessingState, processingOptions )
					.thenAccept( results::addUnique );
		}

		if ( this.uniqueSemantic == ASSERT ) {
			return () -> rowReader
					.reactiveReadRow( rowProcessingState, processingOptions )
					.thenApply( results::addUnique )
					.thenAccept( ReactiveListResultsConsumer::validateUniqueResult );
		}

		return () -> rowReader
				.reactiveReadRow( rowProcessingState, processingOptions )
				.thenAccept( results::add );
	}

	private void end(
			ReactiveValuesResultSet jdbcValues,
			SharedSessionContractImplementor session,
			JdbcValuesSourceProcessingStateStandardImpl jdbcValuesSourceProcessingState,
			ReactiveRowReader<R> rowReader,
			PersistenceContext persistenceContext,
			Throwable ex) {
		try {
			rowReader.finishUp( jdbcValuesSourceProcessingState );
			persistenceContext.afterLoad();
			persistenceContext.initializeNonLazyCollections();
		}
		catch (Throwable e) {
			if ( ex != null ) {
				ex.addSuppressed( e );
				throw (RuntimeException) ex;
			}
			throw e;
		}
		if ( ex != null ) {
			throw (RuntimeException) ex;
		}
	}

	private List<R> finishUp(
			Results<R> results,
			JdbcValuesSourceProcessingStateStandardImpl jdbcValuesSourceProcessingState,
			ReactiveRowReader<R> rowReader, PersistenceContext persistenceContext,
			QueryOptions queryOptions) {
		try {
			rowReader.finishUp( jdbcValuesSourceProcessingState );
			jdbcValuesSourceProcessingState.finishUp();
		}
		finally {
			persistenceContext.getLoadContexts().deregister( jdbcValuesSourceProcessingState );
		}

		final ResultListTransformer<R> resultListTransformer = (ResultListTransformer<R>) queryOptions.getResultListTransformer();
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
			for ( R r : results ) {
				if ( resultJavaType.areEqual( r, result ) ) {
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
