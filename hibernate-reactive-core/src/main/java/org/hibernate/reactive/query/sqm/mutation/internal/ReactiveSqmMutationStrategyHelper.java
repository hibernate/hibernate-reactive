/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal;

import java.sql.PreparedStatement;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.metamodel.mapping.internal.EmbeddedAttributeMapping;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveJdbcMutationExecutor;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.reactive.util.impl.CompletionStages.Completable;
import org.hibernate.sql.ast.tree.delete.DeleteStatement;
import org.hibernate.sql.ast.tree.from.NamedTableReference;
import org.hibernate.sql.ast.tree.from.TableReference;
import org.hibernate.sql.ast.tree.predicate.Predicate;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcOperationQueryMutation;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * @see org.hibernate.query.sqm.mutation.internal.SqmMutationStrategyHelper
 */
public class ReactiveSqmMutationStrategyHelper {

	private ReactiveSqmMutationStrategyHelper() {
	}

	public static CompletionStage<Void> cleanUpCollectionTables(
			EntityMappingType entityDescriptor,
			BiFunction<TableReference, PluralAttributeMapping, Predicate> restrictionProducer,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext) {
		if ( !entityDescriptor.getEntityPersister().hasCollections() ) {
			// none to clean-up
			return voidFuture();
		}

		try {
			final Completable<Void> stage = new Completable<>();
			entityDescriptor
					.visitSubTypeAttributeMappings( attributeMapping -> {
						if ( attributeMapping instanceof PluralAttributeMapping ) {
							cleanUpCollectionTable(
									(PluralAttributeMapping) attributeMapping,
									restrictionProducer,
									jdbcParameterBindings,
									executionContext
							).handle( stage::complete );
						}
						else if ( attributeMapping instanceof EmbeddedAttributeMapping ) {
							cleanUpCollectionTables(
									(EmbeddedAttributeMapping) attributeMapping,
									restrictionProducer,
									jdbcParameterBindings,
									executionContext
							).handle( stage::complete );
						}
						else {
							stage.complete( null, null );
						}
					}
			);
			return stage.getStage();
		}
		catch (Throwable throwable) {
			return failedFuture( throwable );
		}
	}

	private static CompletionStage<Void> cleanUpCollectionTables(
			EmbeddedAttributeMapping attributeMapping,
			BiFunction<TableReference, PluralAttributeMapping, Predicate> restrictionProducer,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext) {
		try {
			final Completable<Void> stage = new Completable<>();
			attributeMapping.visitSubParts(
					modelPart -> {
						if ( modelPart instanceof PluralAttributeMapping ) {
							cleanUpCollectionTable(
									(PluralAttributeMapping) modelPart,
									restrictionProducer,
									jdbcParameterBindings,
									executionContext
							).handle( stage::complete );
						}
						else if ( modelPart instanceof EmbeddedAttributeMapping ) {
							cleanUpCollectionTables(
									(EmbeddedAttributeMapping) modelPart,
									restrictionProducer,
									jdbcParameterBindings,
									executionContext
							).handle( stage::complete );
						}
					},
					null
			);
			return stage.getStage();
		}
		catch (Throwable throwable) {
			return failedFuture( throwable );
		}
	}

	private static CompletionStage<Void> cleanUpCollectionTable(
			PluralAttributeMapping attributeMapping,
			BiFunction<TableReference, PluralAttributeMapping, Predicate> restrictionProducer,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext) {
		final String separateCollectionTable = attributeMapping.getSeparateCollectionTable();
		if ( separateCollectionTable == null ) {
			// one-to-many - update the matching rows in the associated table setting the fk column(s) to null
			// not yet implemented - do nothing
			return voidFuture();
		}

		final SessionFactoryImplementor sessionFactory = executionContext.getSession().getFactory();
		final JdbcServices jdbcServices = sessionFactory.getJdbcServices();

		// element-collection or many-to-many - delete the collection-table row

		final NamedTableReference tableReference = new NamedTableReference(
				separateCollectionTable,
				DeleteStatement.DEFAULT_ALIAS,
				true
		);

		final DeleteStatement sqlAstDelete = new DeleteStatement(
				tableReference,
				restrictionProducer.apply( tableReference, attributeMapping )
		);

		JdbcOperationQueryMutation jdbcDelete = jdbcServices.getJdbcEnvironment()
				.getSqlAstTranslatorFactory()
				.buildMutationTranslator( sessionFactory, sqlAstDelete )
				.translate( jdbcParameterBindings, executionContext.getQueryOptions() );
		return StandardReactiveJdbcMutationExecutor.INSTANCE
				.executeReactive(
						jdbcDelete,
						jdbcParameterBindings,
						executionContext.getSession().getJdbcCoordinator().getStatementPreparer()::prepareStatement,
						ReactiveSqmMutationStrategyHelper::doNothing,
						executionContext
				)
				.thenCompose( CompletionStages::voidFuture );
	}

	private static void doNothing(Integer i, PreparedStatement ps) {
	}
}
