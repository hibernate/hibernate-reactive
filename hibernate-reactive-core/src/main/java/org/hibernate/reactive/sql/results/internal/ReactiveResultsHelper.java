/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.internal;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metamodel.mapping.ModelPart;
import org.hibernate.reactive.sql.results.spi.ReactiveRowReader;
import org.hibernate.spi.NavigablePath;
import org.hibernate.sql.ast.spi.SqlAstCreationContext;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.results.ResultsLogger;
import org.hibernate.sql.results.graph.AssemblerCreationState;
import org.hibernate.sql.results.graph.DomainResultAssembler;
import org.hibernate.sql.results.graph.Initializer;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMapping;
import org.hibernate.sql.results.spi.RowTransformer;

/**
 * @see org.hibernate.sql.results.internal.ResultsHelper
 */
public class ReactiveResultsHelper {

	public static <R> ReactiveRowReader<R> createRowReader(
			ExecutionContext executionContext,
			LockOptions lockOptions,
			RowTransformer<R> rowTransformer,
			Class<R> transformedResultJavaType,
			JdbcValuesMapping jdbcValuesMapping) {
		final SessionFactoryImplementor sessionFactory = executionContext.getSession().getFactory();

		final Map<NavigablePath, Initializer> initializerMap = new LinkedHashMap<>();
		final ReactiveInitializersList.Builder initializersBuilder = new ReactiveInitializersList.Builder();

		final List<DomainResultAssembler<?>> assemblers = jdbcValuesMapping.resolveAssemblers(
				creationState( executionContext, lockOptions, sessionFactory, initializerMap, initializersBuilder )
		);

		logInitializers( initializerMap );

		final ReactiveInitializersList initializersList = initializersBuilder.build( initializerMap );
		return new ReactiveStandardRowReader<>( assemblers, initializersList, rowTransformer, transformedResultJavaType );
	}

	private static AssemblerCreationState creationState(
			ExecutionContext executionContext,
			LockOptions lockOptions,
			SessionFactoryImplementor sessionFactory,
			Map<NavigablePath, Initializer> initializerMap,
			ReactiveInitializersList.Builder initializersBuilder) {
		return new AssemblerCreationState() {

			@Override
			public boolean isScrollResult() {
				return executionContext.isScrollResult();
			}

			@Override
			public LockMode determineEffectiveLockMode(String identificationVariable) {
				return lockOptions.getEffectiveLockMode( identificationVariable );
			}

			@Override
			public Initializer resolveInitializer(
					NavigablePath navigablePath,
					ModelPart fetchedModelPart,
					Supplier<Initializer> producer) {
				final Initializer existing = initializerMap.get( navigablePath );
				if ( existing != null ) {
					if ( fetchedModelPart.getNavigableRole().equals(
							existing.getInitializedPart().getNavigableRole() ) ) {
						ResultsLogger.RESULTS_MESSAGE_LOGGER.tracef(
								"Returning previously-registered initializer : %s",
								existing
						);
						return existing;
					}
				}

				final Initializer initializer = producer.get();
				ResultsLogger.RESULTS_MESSAGE_LOGGER.tracef(
						"Registering initializer : %s",
						initializer
				);

				initializerMap.put( navigablePath, initializer );
				initializersBuilder.addInitializer( initializer );

				return initializer;
			}

			@Override
			public SqlAstCreationContext getSqlAstCreationContext() {
				return sessionFactory;
			}
		};
	}

	private static void logInitializers(Map<NavigablePath, Initializer> initializerMap) {
		if ( ! ResultsLogger.DEBUG_ENABLED ) {
			return;
		}

		ResultsLogger.RESULTS_MESSAGE_LOGGER.debug( "Initializer list" );
		initializerMap.forEach( (navigablePath, initializer) -> {
			ResultsLogger.RESULTS_MESSAGE_LOGGER.debugf(
					"    %s -> %s@%s (%s)",
					navigablePath,
					initializer,
					initializer.hashCode(),
					initializer.getInitializedPart()
			);
		} );
	}
}
