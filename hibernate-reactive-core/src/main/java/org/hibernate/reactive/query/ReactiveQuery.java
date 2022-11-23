package org.hibernate.reactive.query;

/**
 * @see org.hibernate.query.Query
 */
public interface ReactiveQuery<R> extends ReactiveSelectionQuery<R>, ReactiveMutationQuery<R> {
}
