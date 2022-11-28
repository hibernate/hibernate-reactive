/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.internal;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.concurrent.CompletionStage;

import org.hibernate.sql.results.jdbc.spi.JdbcValuesMetadata;

public interface ReactiveResultSetAccess extends JdbcValuesMetadata {
	CompletionStage<ResultSet> getReactiveResultSet();
	CompletionStage<ResultSetMetaData> getReactiveMetadata();
	CompletionStage<Integer> getReactiveColumnCount();

	CompletionStage<JdbcValuesMetadata> resolveJdbcValueMetadata();
}
