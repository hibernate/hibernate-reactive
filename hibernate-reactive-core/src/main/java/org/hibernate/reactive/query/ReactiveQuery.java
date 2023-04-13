/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query;

import java.time.Instant;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.RootGraph;
import org.hibernate.query.BindableType;
import org.hibernate.query.ParameterMetadata;
import org.hibernate.query.QueryParameter;
import org.hibernate.query.ResultListTransformer;
import org.hibernate.query.TupleTransformer;
import org.hibernate.query.spi.QueryOptions;

import jakarta.persistence.CacheRetrieveMode;
import jakarta.persistence.CacheStoreMode;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Parameter;
import jakarta.persistence.TemporalType;

/**
 * @see org.hibernate.query.Query
 */
public interface ReactiveQuery<R> extends ReactiveSelectionQuery<R>, ReactiveMutationQuery<R> {
	String getQueryString();

	@Override
	ReactiveQuery<R> applyGraph(RootGraph<?> graph, GraphSemantic semantic);

	@Override
	default ReactiveQuery<R> applyFetchGraph(RootGraph<?> graph) {
		return applyGraph( graph, GraphSemantic.FETCH );
	}

	@Override
	@SuppressWarnings("UnusedDeclaration")
	default ReactiveQuery<R> applyLoadGraph(RootGraph<?> graph) {
		return applyGraph( graph, GraphSemantic.LOAD );
	}

	String getComment();

	ReactiveQuery<R> setComment(String comment);

	ReactiveQuery<R> addQueryHint(String hint);

	@Override
	LockOptions getLockOptions();

	ReactiveQuery<R> setLockOptions(LockOptions lockOptions);

	@Override
	ReactiveQuery<R> setLockMode(String alias, LockMode lockMode);

	<T> ReactiveQuery<T> setTupleTransformer(TupleTransformer<T> transformer);

	ReactiveQuery<R> setResultListTransformer(ResultListTransformer<R> transformer);

	QueryOptions getQueryOptions();

	ParameterMetadata getParameterMetadata();

	@Override
	ReactiveQuery<R> setParameter(String parameter, Object argument);

	@Override
	<P> ReactiveQuery<R> setParameter(String parameter, P argument, Class<P> type);

	@Override
	<P> ReactiveQuery<R> setParameter(String parameter, P argument, BindableType<P> type);

	/**
	 * Bind an {@link Instant} value to the named query parameter using
	 * just the portion indicated by the given {@link TemporalType}.
	 */
	ReactiveQuery<R> setParameter(String parameter, Instant argument, TemporalType temporalType);

	@Override
	ReactiveQuery<R> setParameter(String parameter, Calendar argument, TemporalType temporalType);

	@Override
	ReactiveQuery<R> setParameter(String parameter, Date argument, TemporalType temporalType);

	/**
	 * Bind the given argument to an ordinal query parameter.
	 * <p>
	 * If the type of the parameter cannot be inferred from the context in
	 * which it occurs, use one of the forms which accepts a "type".
	 *
	 * @see #setParameter(int, Object, Class)
	 * @see #setParameter(int, Object, BindableType)
	 */
	@Override
	ReactiveQuery<R> setParameter(int parameter, Object argument);

	/**
	 * Bind the given argument to an ordinal query parameter using the given
	 * Class reference to attempt to determine the {@link BindableType}
	 * to use.  If unable to determine an appropriate {@link BindableType},
	 * {@link #setParameter(int, Object)} is used.
	 *
	 * @see #setParameter(int, Object, BindableType)
	 */
	<P> ReactiveQuery<R> setParameter(int parameter, P argument, Class<P> type);

	/**
	 * Bind the given argument to an ordinal query parameter using the given
	 * {@link BindableType}.
	 */
	<P> ReactiveQuery<R> setParameter(int parameter, P argument, BindableType<P> type);

	/**
	 * Bind an {@link Instant} value to the ordinal query parameter using
	 * just the portion indicated by the given {@link TemporalType}.
	 */
	ReactiveQuery<R> setParameter(int parameter, Instant argument, TemporalType temporalType);

	/**
	 * {@link jakarta.persistence.Query} override
	 */
	@Override
	ReactiveQuery<R> setParameter(int parameter, Date argument, TemporalType temporalType);

	/**
	 * {@link jakarta.persistence.Query} override
	 */
	@Override
	ReactiveQuery<R> setParameter(int parameter, Calendar argument, TemporalType temporalType);

	<T> ReactiveQuery<R> setParameter(QueryParameter<T> parameter, T argument);

	<P> ReactiveQuery<R> setParameter(QueryParameter<P> parameter, P argument, Class<P> type);

	<P> ReactiveQuery<R> setParameter(QueryParameter<P> parameter, P argument, BindableType<P> type);

	@Override
	<T> ReactiveQuery<R> setParameter(Parameter<T> parameter, T argument);

	@Override
	ReactiveQuery<R> setParameter(Parameter<Calendar> parameter, Calendar argument, TemporalType temporalType);

	@Override
	ReactiveQuery<R> setParameter(Parameter<Date> parameter, Date argument, TemporalType temporalType);

	ReactiveQuery<R> setParameterList(String parameter, @SuppressWarnings("rawtypes") Collection arguments);

	<P> ReactiveQuery<R> setParameterList(String parameter, Collection<? extends P> arguments, Class<P> javaType);

	/**
	 * Bind multiple arguments to a named query parameter using the given
	 * {@link BindableType}.
	 *
	 * @apiNote This is used for binding a list of values to an expression
	 * such as {@code entity.field in (:values)}.
	 *
	 * @return {@code this}, for method chaining
	 */
	<P> ReactiveQuery<R> setParameterList(String parameter, Collection<? extends P> arguments, BindableType<P> type);

 	/**
	 * Bind multiple arguments to a named query parameter.
	 * <p/>
	 * The "type mapping" for the binding is inferred from the type of
	 * the first collection element.
	 *
	 * @apiNote This is used for binding a list of values to an expression s
	 * uch as {@code entity.field in (:values)}.
	 *
	 * @return {@code this}, for method chaining
	 */
	ReactiveQuery<R> setParameterList(String parameter, Object[] values);

	/**
	 * Bind multiple arguments to a named query parameter using the given
	 * Class reference to attempt to determine the {@link BindableType}
	 * to use.  If unable to determine an appropriate {@link BindableType},
	 * {@link #setParameterList(String, Collection)} is used.
	 *
	 * @see #setParameterList(java.lang.String, Object[], BindableType)
	 *
	 * @apiNote This is used for binding a list of values to an expression
	 * such as {@code entity.field in (:values)}.
	 *
	 * @return {@code this}, for method chaining
	 */
	<P> ReactiveQuery<R> setParameterList(String parameter, P[] arguments, Class<P> javaType);


	/**
	 * Bind multiple arguments to a named query parameter using the given
	 * {@link BindableType}.
	 *
	 * @apiNote This is used for binding a list of values to an expression
	 * such as {@code entity.field in (:values)}.
	 *
	 * @return {@code this}, for method chaining
	 */
	<P> ReactiveQuery<R> setParameterList(String parameter, P[] arguments, BindableType<P> type);

	/**
	 * Bind multiple arguments to an ordinal query parameter.
	 * <p/>
	 * The "type mapping" for the binding is inferred from the type of
	 * the first collection element.
	 *
	 * @apiNote This is used for binding a list of values to an expression
	 * such as {@code entity.field in (:values)}.
	 *
	 * @return {@code this}, for method chaining
	 */
	ReactiveQuery<R> setParameterList(int parameter, @SuppressWarnings("rawtypes") Collection arguments);

	/**
	 * Bind multiple arguments to an ordinal query parameter using the given
	 * Class reference to attempt to determine the {@link BindableType}
	 * to use.  If unable to determine an appropriate {@link BindableType},
	 * {@link #setParameterList(String, Collection)} is used.
	 *
	 * @see #setParameterList(int, Collection, BindableType)
	 *
	 * @apiNote This is used for binding a list of values to an expression
	 * such as {@code entity.field in (:values)}.
	 *
	 * @return {@code this}, for method chaining
	 */
	<P> ReactiveQuery<R> setParameterList(int parameter, Collection<? extends P> arguments, Class<P> javaType);

	/**
	 * Bind multiple arguments to an ordinal query parameter using the given
	 * {@link BindableType}.
	 *
	 * @apiNote This is used for binding a list of values to an expression
	 * such as {@code entity.field in (:values)}.
	 *
	 * @return {@code this}, for method chaining
	 */
	<P> ReactiveQuery<R> setParameterList(int parameter, Collection<? extends P> arguments, BindableType<P> type);

	/**
	 * Bind multiple arguments to an ordinal query parameter.
	 * <p>
	 * The "type mapping" for the binding is inferred from the type of the
	 * first collection element.
	 *
	 * @apiNote This is used for binding a list of values to an expression
	 * such as {@code entity.field in (:values)}.
	 *
	 * @return {@code this}, for method chaining
	 */
	ReactiveQuery<R> setParameterList(int parameter, Object[] arguments);

	/**
	 * Bind multiple arguments to an ordinal query parameter using the given
	 * {@link Class} reference to attempt to determine the {@link BindableType}
	 * to use. If unable to determine an appropriate {@link BindableType},
	 * {@link #setParameterList(String, Collection)} is used.
	 *
	 * @see #setParameterList(int, Object[], BindableType)
	 *
	 * @apiNote This is used for binding a list of values to an expression
	 * such as {@code entity.field in (:values)}.
	 *
	 * @return {@code this}, for method chaining
	 */
	<P> ReactiveQuery<R> setParameterList(int parameter, P[] arguments, Class<P> javaType);

	/**
	 * Bind multiple arguments to an ordinal query parameter using the given
	 * {@link BindableType}.
	 *
	 * @apiNote This is used for binding a list of values to an expression
	 * such as {@code entity.field in (:values)}.
	 *
	 * @return {@code this}, for method chaining
	 */
	<P> ReactiveQuery<R> setParameterList(int parameter, P[] arguments, BindableType<P> type);

	/**
	 * Bind multiple arguments to the query parameter represented by the given
	 * {@link QueryParameter}.
	 * <p>
	 * The type of the parameter is inferred from the context in which it occurs,
	 * and from the type of the first given argument.
	 *
	 * @param parameter the parameter memento
	 * @param arguments a collection of arguments
	 *
	 * @return {@code this}, for method chaining
	 */
	<P> ReactiveQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> arguments);

	/**
	 * Bind multiple arguments to the query parameter represented by the given
	 * {@link QueryParameter} using the given Class reference to attempt to
	 * determine the {@link BindableType} to use. If unable to determine an
	 * appropriate {@link BindableType}, {@link #setParameterList(String, Collection)}
	 * is used.
	 *
	 * @see #setParameterList(QueryParameter, java.util.Collection, BindableType)
	 *
	 * @apiNote This is used for binding a list of values to an expression such
	 * as {@code entity.field in (:values)}.
	 *
	 * @return {@code this}, for method chaining
	 */
	<P> ReactiveQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> arguments, Class<P> javaType);

	/**
	 * Bind multiple arguments to the query parameter represented by the given
	 * {@link QueryParameter}, inferring the {@link BindableType}.
	 * <p>
	 * The "type mapping" for the binding is inferred from the type of the first
	 * collection element.
	 *
	 * @apiNote This is used for binding a list of values to an expression such
	 * as {@code entity.field in (:values)}.
	 *
	 * @return {@code this}, for method chaining
	 */
	<P> ReactiveQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> arguments, BindableType<P> type);

	/**
	 * Bind multiple arguments to the query parameter represented by the
	 * given {@link QueryParameter}
	 * <p>
	 * The type of the parameter is inferred between the context in which it
	 * occurs, the type associated with the QueryParameter and the type of
	 * the first given argument.
	 *
	 * @param parameter the parameter memento
	 * @param arguments a collection of arguments
	 *
	 * @return {@code this}, for method chaining
	 */
	<P> ReactiveQuery<R> setParameterList(QueryParameter<P> parameter, P[] arguments);

	/**
	 * Bind multiple arguments to the query parameter represented by the
	 * given {@link QueryParameter} using the given Class reference to attempt
	 * to determine the {@link BindableType} to use.  If unable to
	 * determine an appropriate {@link BindableType},
	 * {@link #setParameterList(String, Collection)} is used
	 *
	 * @see #setParameterList(QueryParameter, Object[], BindableType)
	 *
	 * @apiNote This is used for binding a list of values to an expression such
	 * as {@code entity.field in (:values)}.
	 *
	 * @return {@code this}, for method chaining
	 */
	<P> ReactiveQuery<R> setParameterList(QueryParameter<P> parameter, P[] arguments, Class<P> javaType);

	/**
	 * Bind multiple arguments to the query parameter represented by the
	 * given {@link QueryParameter}, inferring the {@link BindableType}.
	 * <p>
	 * The "type mapping" for the binding is inferred from the type of
	 * the first collection element
	 *
	 * @apiNote This is used for binding a list of values to an expression such
	 * as {@code entity.field in (:values)}.
	 *
	 * @return {@code this}, for method chaining
	 */
	<P> ReactiveQuery<R> setParameterList(QueryParameter<P> parameter, P[] arguments, BindableType<P> type);

	/**
	 * Bind the property values of the given bean to named parameters of the query,
	 * matching property names with parameter names and mapping property types to
	 * Hibernate types using heuristics.
	 *
	 * @param bean any JavaBean or POJO
	 *
	 * @return {@code this}, for method chaining
	 */
	ReactiveQuery<R> setProperties(Object bean);

	/**
	 * Bind the values of the given Map for each named parameters of the query,
	 * matching key names with parameter names and mapping value types to
	 * Hibernate types using heuristics.
	 *
	 * @param bean a {@link Map} of names to arguments
	 *
	 * @return {@code this}, for method chaining
	 */
	ReactiveQuery<R> setProperties(@SuppressWarnings("rawtypes") Map bean);


	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// covariant overrides - CommonQueryContract

	@Override
	ReactiveQuery<R> setHibernateFlushMode(FlushMode flushMode);

	@Override
	ReactiveQuery<R> setCacheable(boolean cacheable);

	@Override
	ReactiveQuery<R> setCacheRegion(String cacheRegion);

	@Override
	ReactiveQuery<R> setCacheMode(CacheMode cacheMode);

	@Override
	ReactiveQuery<R> setCacheStoreMode(CacheStoreMode cacheStoreMode);

	@Override
	ReactiveQuery<R> setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode);

	@Override
	ReactiveQuery<R> setTimeout(int timeout);

	@Override
	ReactiveQuery<R> setFetchSize(int fetchSize);

	@Override
	ReactiveQuery<R> setReadOnly(boolean readOnly);


	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// covariant overrides - jakarta.persistence.Query/TypedQuery

	@Override
	ReactiveQuery<R> setMaxResults(int maxResult);

	@Override
	ReactiveQuery<R> setFirstResult(int startPosition);

	@Override
	ReactiveQuery<R> setHint(String hintName, Object value);

	@Override
	ReactiveQuery<R> setFlushMode(FlushModeType flushMode);

	@Override
	ReactiveQuery<R> setLockMode(LockModeType lockMode);

}
