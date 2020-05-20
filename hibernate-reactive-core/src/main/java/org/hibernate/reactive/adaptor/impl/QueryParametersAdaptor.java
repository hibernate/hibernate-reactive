package org.hibernate.reactive.adaptor.impl;

import org.hibernate.JDBCException;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.type.Type;

import java.sql.SQLException;

public class QueryParametersAdaptor {

	public static Object[] toParameterArray(QueryParameters queryParameters, SessionImplementor session) {
		PreparedStatementAdaptor adaptor = new PreparedStatementAdaptor();
		Type[] types = queryParameters.getPositionalParameterTypes();
		Object[] values = queryParameters.getPositionalParameterValues();
		int n = 1;
		for (int i = 0; i < types.length; i++) {
			Type type = types[i];
			Object value = values[i];
			try {
				type.nullSafeSet(adaptor, value, n, session);
				n += type.getColumnSpan( session.getSessionFactory() );
			}
			catch (SQLException e) {
				//can never happen
				throw new JDBCException("error binding parameters", e);
			}
		}
		return adaptor.getParametersAsArray();
	}

}
