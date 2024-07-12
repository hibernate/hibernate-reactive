/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.spi;

import java.util.concurrent.CompletionStage;

import org.hibernate.reactive.sql.exec.spi.ReactiveRowProcessingState;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesSourceProcessingOptions;
import org.hibernate.sql.results.spi.RowReader;

public interface ReactiveRowReader<R> extends RowReader<R> {

	CompletionStage<R> reactiveReadRow(ReactiveRowProcessingState processingState, JdbcValuesSourceProcessingOptions options);
}
