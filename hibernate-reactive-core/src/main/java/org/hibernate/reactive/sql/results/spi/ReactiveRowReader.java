/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.spi;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.sql.exec.spi.ReactiveRowProcessingState;
import org.hibernate.reactive.sql.results.internal.ReactiveInitializersList;
import org.hibernate.sql.results.internal.InitializersList;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesSourceProcessingOptions;
import org.hibernate.sql.results.spi.RowReader;

public interface ReactiveRowReader<R> extends RowReader<R> {

	Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	CompletionStage<R> reactiveReadRow(ReactiveRowProcessingState processingState, JdbcValuesSourceProcessingOptions options);

	@Override
	default InitializersList getInitializersList() {
		throw LOG.nonReactiveMethodCall( "getReactiveInitializersList" );
	}

	ReactiveInitializersList getReactiveInitializersList();
}
