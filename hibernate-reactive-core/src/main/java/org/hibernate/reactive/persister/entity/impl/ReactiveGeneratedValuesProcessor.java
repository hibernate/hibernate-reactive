/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.impl;

import java.util.List;
import java.util.concurrent.CompletionStage;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.ast.internal.NoCallbackExecutionContext;
import org.hibernate.metamodel.mapping.AttributeMapping;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveSelectExecutor;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.sql.ast.SqlAstTranslatorFactory;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.internal.JdbcParameterBindingsImpl;
import org.hibernate.sql.exec.spi.JdbcOperationQuerySelect;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.exec.spi.JdbcParametersList;

/**
 * A reactive version of {@link org.hibernate.metamodel.mapping.internal.GeneratedValuesProcessor}
 */
class ReactiveGeneratedValuesProcessor {

    private final SelectStatement selectStatement;
    private final List<AttributeMapping> generatedValuesToSelect;
    private final JdbcParametersList jdbcParameters;

    private final EntityMappingType entityDescriptor;
    private final SessionFactoryImplementor sessionFactory;

    ReactiveGeneratedValuesProcessor(SelectStatement selectStatement,
                                     List<AttributeMapping> generatedValuesToSelect,
                                     JdbcParametersList jdbcParameters,
                                     EntityMappingType entityDescriptor,
                                     SessionFactoryImplementor sessionFactory) {
        this.selectStatement = selectStatement;
        this.generatedValuesToSelect = generatedValuesToSelect;
        this.jdbcParameters = jdbcParameters;
        this.entityDescriptor = entityDescriptor;
        this.sessionFactory = sessionFactory;
    }

    CompletionStage<Void> processGeneratedValues(Object id,
                                                        Object entity,
                                                        Object[] state,
                                                        SharedSessionContractImplementor session) {

        final JdbcServices jdbcServices = sessionFactory.getJdbcServices();
        final JdbcEnvironment jdbcEnvironment = jdbcServices.getJdbcEnvironment();
        final SqlAstTranslatorFactory sqlAstTranslatorFactory = jdbcEnvironment.getSqlAstTranslatorFactory();

        final JdbcParameterBindings jdbcParameterBindings = getJdbcParameterBindings(id, session);

        final JdbcOperationQuerySelect jdbcSelect = sqlAstTranslatorFactory
                .buildSelectTranslator( sessionFactory, selectStatement ).translate(jdbcParameterBindings, QueryOptions.NONE);

        return StandardReactiveSelectExecutor.INSTANCE
                .list(jdbcSelect, jdbcParameterBindings, new NoCallbackExecutionContext( session ), r -> r, ReactiveListResultsConsumer.UniqueSemantic.FILTER)
                .thenAccept(l -> {
                    assert l.size() == 1;
                    setEntityAttributes( entity, state, l.get(0) );
                }) ;
    }

    private JdbcParameterBindings getJdbcParameterBindings(Object id, SharedSessionContractImplementor session) {
        final JdbcParameterBindings jdbcParamBindings = new JdbcParameterBindingsImpl( jdbcParameters.size() );
        int offset = jdbcParamBindings.registerParametersForEachJdbcValue(
                id,
                entityDescriptor.getIdentifierMapping(),
                jdbcParameters,
                session
        );
        assert offset == jdbcParameters.size();
        return jdbcParamBindings;
    }

    private void setEntityAttributes(Object entity, Object[] state, Object[] selectionResults) {
        for ( int i = 0; i < generatedValuesToSelect.size(); i++ ) {
            final AttributeMapping attribute = generatedValuesToSelect.get( i );
            final Object generatedValue = selectionResults[i];
            state[ attribute.getStateArrayPosition() ] = generatedValue;
            attribute.getAttributeMetadata().getPropertyAccess().getSetter().set( entity, generatedValue );
        }
    }
}
