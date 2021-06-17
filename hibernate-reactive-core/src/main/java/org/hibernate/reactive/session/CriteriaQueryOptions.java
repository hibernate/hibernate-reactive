/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session;

import org.hibernate.query.criteria.internal.SelectionImplementor;
import org.hibernate.query.criteria.internal.ValueHandlerFactory;
import org.hibernate.query.criteria.internal.compile.InterpretedParameterMetadata;
import org.hibernate.type.Type;

import java.util.List;

public interface CriteriaQueryOptions  {

	default void validate(Type[] returnTypes) {}

	default SelectionImplementor<?> getSelection() { return null; }

	InterpretedParameterMetadata getParameterMetadata();

	/**
	 * Get the conversions for the individual tuples in the query results.
	 *
	 * @return Value conversions to be applied to the JPA QL results
	 */
	@SuppressWarnings("rawtypes")
	default List<ValueHandlerFactory.ValueHandler> getValueHandlers() { return null; }

}
