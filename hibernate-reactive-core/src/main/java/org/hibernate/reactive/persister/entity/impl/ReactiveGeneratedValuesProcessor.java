/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.impl;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.loader.ast.internal.NoCallbackExecutionContext;
import org.hibernate.metamodel.mapping.AttributeMapping;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveSelectExecutor;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.internal.JdbcParameterBindingsImpl;
import org.hibernate.sql.exec.spi.JdbcOperationQuerySelect;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.exec.spi.JdbcParametersList;

import static org.hibernate.internal.util.NullnessUtil.castNonNull;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * A reactive version of {@link org.hibernate.metamodel.mapping.internal.GeneratedValuesProcessor}
 */
class ReactiveGeneratedValuesProcessor {

    private final SelectStatement selectStatement;
    private final List<AttributeMapping> generatedValuesToSelect;
    private final JdbcParametersList jdbcParameters;
    private final JdbcOperationQuerySelect jdbcSelect;
    private final EntityMappingType entityDescriptor;

    ReactiveGeneratedValuesProcessor(SelectStatement selectStatement,
                                     JdbcOperationQuerySelect jdbcSelect,
                                     List<AttributeMapping> generatedValuesToSelect,
                                     JdbcParametersList jdbcParameters,
                                     EntityMappingType entityDescriptor) {
        this.selectStatement = selectStatement;
        this.jdbcSelect = jdbcSelect;
        this.generatedValuesToSelect = generatedValuesToSelect;
        this.jdbcParameters = jdbcParameters;
        this.entityDescriptor = entityDescriptor;
    }

    CompletionStage<Void> processGeneratedValues(Object id, Object entity, Object[] state, GeneratedValues generatedValues, SharedSessionContractImplementor session) {
        if ( hasActualGeneratedValuesToSelect( session, entity ) ) {
            if ( selectStatement != null ) {
                return executeSelect( id, session )
                        .thenAccept( l -> {
                            assert l.size() == 1;
                            setEntityAttributes( entity, state, l.get( 0 ) );
                        } );
            }
            else {
                castNonNull( generatedValues );
                final List<Object> results = generatedValues.getGeneratedValues( generatedValuesToSelect );
                setEntityAttributes( entity, state, results.toArray( new Object[0] ) );
                return voidFuture();
            }
        }
        return voidFuture();
    }

    private CompletionStage<List<Object[]>> executeSelect(Object id, SharedSessionContractImplementor session) {
        return StandardReactiveSelectExecutor.INSTANCE
                .list(
                        jdbcSelect,
                        getJdbcParameterBindings( id, session ),
                        new NoCallbackExecutionContext( session ),
                        r -> r,
                        ReactiveListResultsConsumer.UniqueSemantic.FILTER
                );
    }

    private boolean hasActualGeneratedValuesToSelect(SharedSessionContractImplementor session, Object entity) {
        for ( AttributeMapping attributeMapping : generatedValuesToSelect ) {
            if ( attributeMapping.getGenerator().generatedOnExecution( entity, session ) ) {
                return true;
            }
        }
        return false;
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
