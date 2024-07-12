/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.internal;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.reactive.sql.exec.spi.ReactiveValuesResultSet;
import org.hibernate.reactive.sql.results.spi.ReactiveRowReader;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMappingResolution;
import org.hibernate.sql.results.spi.RowTransformer;

/**
 * @see org.hibernate.sql.results.internal.ResultsHelper
 */
public class ReactiveResultsHelper {

	public static <R> ReactiveRowReader<R> createRowReader(
			SessionFactoryImplementor sessionFactory,
			RowTransformer<R> rowTransformer,
			Class<R> transformedResultJavaType,
			ReactiveValuesResultSet resultSet) {
		final JdbcValuesMappingResolution jdbcValuesMappingResolution = resultSet
				.getValuesMapping().resolveAssemblers( sessionFactory );
		return new ReactiveStandardRowReader<>( jdbcValuesMappingResolution, rowTransformer, transformedResultJavaType );
	}
}
