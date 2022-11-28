/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.spi;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hibernate.internal.FilterJdbcParameter;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.exec.spi.JdbcLockStrategy;
import org.hibernate.sql.exec.spi.JdbcOperation;
import org.hibernate.sql.exec.spi.JdbcParameterBinder;
import org.hibernate.sql.exec.spi.JdbcParameterBinding;
import org.hibernate.sql.exec.spi.JdbcSelect;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMappingProducer;

public class ReactiveJdbcSelect extends JdbcSelect implements JdbcOperation {

	public ReactiveJdbcSelect(
			String sql,
			List<JdbcParameterBinder> parameterBinders,
			JdbcValuesMappingProducer jdbcValuesMappingProducer,
			Set<String> affectedTableNames,
			Set<FilterJdbcParameter> filterJdbcParameters) {
		super( sql, parameterBinders, jdbcValuesMappingProducer, affectedTableNames, filterJdbcParameters );
	}

	public ReactiveJdbcSelect(
			String sql,
			List<JdbcParameterBinder> parameterBinders,
			JdbcValuesMappingProducer jdbcValuesMappingProducer,
			Set<String> affectedTableNames,
			Set<FilterJdbcParameter> filterJdbcParameters,
			int rowsToSkip,
			int maxRows,
			Map<JdbcParameter, JdbcParameterBinding> appliedParameters,
			JdbcLockStrategy jdbcLockStrategy,
			JdbcParameter offsetParameter,
			JdbcParameter limitParameter) {
		super(
				sql,
				parameterBinders,
				jdbcValuesMappingProducer,
				affectedTableNames,
				filterJdbcParameters,
				rowsToSkip,
				maxRows,
				appliedParameters,
				jdbcLockStrategy,
				offsetParameter,
				limitParameter
		);
	}
}
