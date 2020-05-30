/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.adaptor.impl;

import org.hibernate.JDBCException;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.param.ParameterSpecification;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;

public class QueryParametersAdaptor {

	public static Object[] toIdParameterArray(
			final Serializable id,
			final Type type,
			final SharedSessionContractImplementor session) {

		final PreparedStatementAdaptor adaptor = new PreparedStatementAdaptor();
		try {
			type.nullSafeSet( adaptor, id, 1, session );
			return adaptor.getParametersAsArray();
		}
		catch (SQLException e) {
			//can never happen
			throw new JDBCException("error binding parameters", e);
		}
	}

	public static Object[] toParameterArray(QueryParameters queryParameters, SharedSessionContractImplementor session) {

		final PreparedStatementAdaptor adaptor = new PreparedStatementAdaptor();
		try {
			Type[] types = queryParameters.getPositionalParameterTypes();
			Object[] values = queryParameters.getPositionalParameterValues();
			int n = 1;
			for (int i = 0; i < types.length; i++) {
				Type type = types[i];
				Object value = values[i];
				type.nullSafeSet(adaptor, value, n, session);
				n += type.getColumnSpan(session.getFactory());
			}
			return adaptor.getParametersAsArray();
		}
		catch (SQLException e) {
			//can never happen
			throw new JDBCException("error binding parameters", e);
		}
	}

	public static Object[] toParameterArray(
			QueryParameters queryParameters,
			List<ParameterSpecification> parameterSpecifications,
			SharedSessionContractImplementor session) {
		final PreparedStatementAdaptor adaptor = new PreparedStatementAdaptor();
		try {
			int pos = 1;
			for (ParameterSpecification parameterSpecification : parameterSpecifications) {
				pos += parameterSpecification.bind(adaptor, queryParameters, session, pos);
			}
			return adaptor.getParametersAsArray();
		}
		catch (SQLException e) {
			//can never happen
			throw new JDBCException("error binding parameters", e);
		}
	}
}
