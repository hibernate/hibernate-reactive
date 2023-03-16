/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.mutation.internal;

import java.sql.PreparedStatement;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.metamodel.mapping.internal.EmbeddedAttributeMapping;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveJdbcMutationExecutor;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.sql.ast.tree.delete.DeleteStatement;
import org.hibernate.sql.ast.tree.from.NamedTableReference;
import org.hibernate.sql.ast.tree.from.TableReference;
import org.hibernate.sql.ast.tree.predicate.Predicate;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcOperationQueryDelete;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * @see org.hibernate.query.sqm.mutation.internal.SqmMutationStrategyHelper
 */
public class ReactiveSqmMutationStrategyHelper {
	/**
	 * Singleton access
	 */
	public static final ReactiveSqmMutationStrategyHelper INSTANCE = new ReactiveSqmMutationStrategyHelper();

	private ReactiveSqmMutationStrategyHelper() {
	}

	public static CompletionStage<Void> visitCollectionTables(EntityMappingType entityDescriptor, Consumer<PluralAttributeMapping> consumer) {
		if ( !entityDescriptor.getEntityPersister().hasCollections() ) {
			// none to clean-up
			return voidFuture();
		}

		final CompletableFuture<Void> stage = new CompletableFuture();
		try {
			entityDescriptor.visitSubTypeAttributeMappings(
					attributeMapping -> {
						if ( attributeMapping instanceof PluralAttributeMapping ) {
							try {
								consumer.accept( (PluralAttributeMapping) attributeMapping );
								complete( stage, null );
							}
							catch (Throwable throwable) {
								complete( stage, throwable );
							}
						}
						else if ( attributeMapping instanceof EmbeddedAttributeMapping ) {
							visitCollectionTables( (EmbeddedAttributeMapping) attributeMapping, consumer )
									.whenComplete( (v, throwable) -> complete( stage, throwable ) );
						}
						else {
							complete( stage, null );
						}
					} );
			return stage;
		}
		catch (Throwable throwable) {
			complete( stage, throwable );
			return stage;
		}
	}

	private static CompletionStage<Void> visitCollectionTables(EmbeddedAttributeMapping attributeMapping, Consumer<PluralAttributeMapping> consumer) {
		final CompletableFuture<Void> stage = new CompletableFuture<>();

		try {
			attributeMapping.visitSubParts(
					modelPart -> {
						if ( modelPart instanceof PluralAttributeMapping ) {
							try {
								consumer.accept( (PluralAttributeMapping) modelPart );
								complete( stage, null );
							}
							catch (Throwable throwable) {
								complete( stage, throwable );
							}
						}
						else if ( modelPart instanceof EmbeddedAttributeMapping ) {
							visitCollectionTables( (EmbeddedAttributeMapping) modelPart, consumer )
									.whenComplete( (v, throwable) -> complete( stage, throwable ) );
						}
						else {
							complete( stage, null );
						}
					},
					null
			);
			return stage;
		}
		catch (Throwable t) {
			complete( stage, t );
			return stage;
		}
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

		final CompletableFuture<Void> stage = new CompletableFuture<>();
		try {
			entityDescriptor.visitSubTypeAttributeMappings(
					attributeMapping -> {
						if ( attributeMapping instanceof PluralAttributeMapping ) {
							cleanUpCollectionTable(
									(PluralAttributeMapping) attributeMapping,
									entityDescriptor,
									restrictionProducer,
									jdbcParameterBindings,
									executionContext
							).whenComplete( (v, throwable) -> complete( stage, throwable ) );
						}
						else if ( attributeMapping instanceof EmbeddedAttributeMapping ) {
							cleanUpCollectionTables(
									(EmbeddedAttributeMapping) attributeMapping,
									entityDescriptor,
									restrictionProducer,
									jdbcParameterBindings,
									executionContext
							).whenComplete( (v, throwable) -> complete( stage, throwable ) );
						}
						else {
							complete( stage, null );
						}
					}
			);
			return stage;
		}
		catch (Throwable throwable) {
			complete( stage, throwable );
			return stage;
		}
	}

	private static CompletionStage<Void> cleanUpCollectionTables(
			EmbeddedAttributeMapping attributeMapping,
			EntityMappingType entityDescriptor,
			BiFunction<TableReference, PluralAttributeMapping, Predicate> restrictionProducer,
			JdbcParameterBindings jdbcParameterBindings,
			ExecutionContext executionContext) {
		final CompletableFuture<Void> stage = new CompletableFuture<>();
		try {
			attributeMapping.visitSubParts(
					modelPart -> {
						if ( modelPart instanceof PluralAttributeMapping ) {
							cleanUpCollectionTable(
									(PluralAttributeMapping) modelPart,
									entityDescriptor,
									restrictionProducer,
									jdbcParameterBindings,
									executionContext
							);
						}
						else if ( modelPart instanceof EmbeddedAttributeMapping ) {
							cleanUpCollectionTables(
									(EmbeddedAttributeMapping) modelPart,
									entityDescriptor,
									restrictionProducer,
									jdbcParameterBindings,
									executionContext
							);
						}
					},
					null
			);
			return stage;
		}
		catch (Throwable throwable) {
			complete( stage, throwable );
			return stage;
		}
	}

	private static CompletionStage<Void> cleanUpCollectionTable(
			PluralAttributeMapping attributeMapping,
			EntityMappingType entityDescriptor,
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

		JdbcOperationQueryDelete jdbcDelete = jdbcServices.getJdbcEnvironment()
				.getSqlAstTranslatorFactory()
				.buildDeleteTranslator( sessionFactory, sqlAstDelete )
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

	private static void complete(CompletableFuture<Void> stage, Throwable throwable) {
		if ( throwable == null ) {
			stage.complete( null );
		}
		else {
			stage.completeExceptionally( throwable );
		}
	}

	private static void doNothing(Integer i, PreparedStatement ps) {
	}
}
