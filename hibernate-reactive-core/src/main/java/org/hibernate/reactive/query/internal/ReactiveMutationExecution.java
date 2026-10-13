/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.internal;

import java.util.HashSet;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.ExceptionConverter;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.IllegalQueryOperationException;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.reactive.util.internal.CompletionStages;

import static org.hibernate.reactive.util.internal.CompletionStages.failedFuture;

/**
 * Emulate {@link org.hibernate.query.internal.AbstractQuery#executeMutation(java.util.IntSupplier)}.
 * <p>
 * The Hibernate Reactive implementations of the mutation queries,
 * {@link org.hibernate.reactive.query.sqm.internal.ReactiveMutationQueryImpl} and
 * {@link org.hibernate.reactive.query.sql.internal.ReactiveNativeQueryImpl},
 * extend different classes in ORM and cannot share a superclass.
 * This approach allows us to avoid duplicating code.
 * </p>
 */
public final class ReactiveMutationExecution {

	private ReactiveMutationExecution() {
	}

	/**
	 * Execute a mutation query, the reactive equivalent of
	 * {@link org.hibernate.query.internal.AbstractQuery#executeMutation(java.util.IntSupplier)}:
	 * <ol>
	 *     <li>{@code beforeQuery} prepares the session, like
	 *     {@link org.hibernate.query.internal.AbstractCommonQueryContract#beforeQueryHandlingFetchProfiles()}</li>
	 *     <li>{@code mutation} executes the query</li>
	 *     <li>{@code afterQuery} always resets the session, like
	 *     {@link org.hibernate.query.internal.AbstractCommonQueryContract#afterQueryHandlingFetchProfiles(boolean, HashSet)}
	 *     in the {@code finally} block of ORM</li>
	 *     <li>a failure is converted like in the {@code catch} blocks of ORM, see {@code convertException}</li>
	 * </ol>
	 *
	 * @param beforeQuery prepares the session for the execution of the query,
	 * and returns the fetch profiles to restore after the execution
	 * @param mutation executes the query
	 * @param afterQuery resets the session, receives {@code true} if the execution succeeded
	 * and the fetch profiles returned by {@code beforeQuery}
	 * @param exceptionConverter converts the exceptions of the execution
	 *
	 * @return the number of affected rows
	 */
	public static CompletionStage<Integer> executeReactiveMutation(
			Supplier<CompletionStage<HashSet<String>>> beforeQuery,
			Supplier<CompletionStage<Integer>> mutation,
			BiConsumer<Boolean, HashSet<String>> afterQuery,
			ExceptionConverter exceptionConverter) {
		return beforeQuery.get()
				.thenCompose( fetchProfiles -> execute( mutation )
						.handle( CompletionStages::handle )
						.thenCompose( handler -> {
							afterQuery.accept( !handler.hasFailed(), fetchProfiles );
							return handler.hasFailed()
									? failedFuture( convertException( handler.getThrowable(), exceptionConverter ) )
									: handler.getResultAsCompletionStage();
						} )
				);
	}

	/**
	 * Enable and disable the fetch profiles of the query, like
	 * {@link org.hibernate.query.internal.AbstractCommonQueryContract#beforeQueryHandlingFetchProfiles()}
	 * does after {@code beforeQuery()}.
	 * <p>
	 * Pass the result to
	 * {@link org.hibernate.query.internal.AbstractCommonQueryContract#afterQueryHandlingFetchProfiles(boolean, HashSet)}
	 * to restore the fetch profiles of the session after the execution.
	 * </p>
	 *
	 * @return the fetch profiles that were enabled before the change
	 *
	 * @see org.hibernate.engine.spi.LoadQueryInfluencers#adjustFetchProfiles
	 */
	public static HashSet<String> adjustFetchProfiles(QueryOptions options, SharedSessionContractImplementor session) {
		return session.getLoadQueryInfluencers()
				.adjustFetchProfiles( options.getDisabledFetchProfiles(), options.getEnabledFetchProfiles() );
	}

	/**
	 * Execute the mutation, returning a failed stage instead of throwing an exception,
	 * so that {@code afterQuery} always runs, like the {@code finally} block in
	 * {@link org.hibernate.query.internal.AbstractQuery#executeMutation(java.util.IntSupplier)}.
	 */
	private static CompletionStage<Integer> execute(Supplier<CompletionStage<Integer>> mutation) {
		try {
			return mutation.get();
		}
		catch (RuntimeException e) {
			return failedFuture( e );
		}
	}

	/**
	 * Convert the failure of the execution like the {@code catch} blocks of
	 * {@link org.hibernate.query.internal.AbstractQuery#executeMutation(java.util.IntSupplier)}:
	 * <ul>
	 *     <li>an {@link IllegalQueryOperationException} becomes an {@link IllegalStateException}</li>
	 *     <li>a {@link HibernateException} is converted by the {@link ExceptionConverter}</li>
	 *     <li>any other exception is returned unchanged</li>
	 * </ul>
	 * A {@link CompletionException} is unwrapped first, because the reactive execution
	 * can wrap the original exception.
	 */
	private static Throwable convertException(Throwable error, ExceptionConverter exceptionConverter) {
		final Throwable cause = error instanceof CompletionException && error.getCause() != null
				? error.getCause()
				: error;
		if ( cause instanceof IllegalQueryOperationException ) {
			return new IllegalStateException( cause );
		}
		if ( cause instanceof HibernateException hibernateException ) {
			return exceptionConverter.convert( hibernateException );
		}
		return cause;
	}
}
