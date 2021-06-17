/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.engine.ResultSetMappingDefinition;
import org.hibernate.engine.query.spi.sql.*;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.reactive.common.ResultSetMapping;

class ResultSetMappings {

    static <T> ResultSetMapping<T> resultSetMapping(Class<T> resultType, String mappingName,
                                                    SessionFactoryImplementor factory) {
        ResultSetMappingDefinition mapping = factory.getNamedQueryRepository()
                .getResultSetMappingDefinition(mappingName);
        if (mapping==null) {
            throw new IllegalArgumentException("result set mapping does not exist: " + mappingName);
        }

        if ( resultType !=null ) {
            Class<?> mappedResultType = getResultType( mapping, factory );
            if ( !resultType.equals(mappedResultType) ) {
                throw new IllegalArgumentException("incorrect result type for result set mapping: "
                        + mappingName + " has type " + mappedResultType.getName() );
            }
        }

        return new ResultSetMapping<T>() {
            @Override
            public String getName() {
                return mappingName;
            }
            @Override
            public Class<T> getResultType() {
                return resultType;
            }
        };
    }

    private static Class<?> getResultType(ResultSetMappingDefinition mapping, SessionFactoryImplementor factory) {
        Class<?> mappedResultType = null;
        for ( NativeSQLQueryReturn queryReturn: mapping.getQueryReturns() ) {
            if (queryReturn instanceof NativeSQLQueryScalarReturn) {
                if (mappedResultType != null) {
                    return Object[].class;
                }
                else {
                    mappedResultType = ((NativeSQLQueryScalarReturn) queryReturn).getType().getReturnedClass();
                }
            }
            else if (queryReturn instanceof NativeSQLQueryRootReturn) {
                if (mappedResultType != null) {
                    return Object[].class;
                }
                else {
                    NativeSQLQueryRootReturn entityReturn = (NativeSQLQueryRootReturn) queryReturn;
                    String entityName = entityReturn.getReturnEntityName();
                    mappedResultType = factory.getMetamodel().entityPersister(entityName).getMappedClass();
                }
            }
            else if (queryReturn instanceof NativeSQLQueryCollectionReturn) {
                if (mappedResultType != null) {
                    return Object[].class;
                }
                else {
                    NativeSQLQueryCollectionReturn collectionReturn = (NativeSQLQueryCollectionReturn) queryReturn;
                    String entityName = collectionReturn.getOwnerEntityName();
                    String propertyName = collectionReturn.getOwnerProperty();
                    String role = entityName + '.' + propertyName;
                    mappedResultType = factory.getMetamodel().collectionPersister(role).getElementClass();
                }
            }
            else if (queryReturn instanceof NativeSQLQueryConstructorReturn) {
                if (mappedResultType != null) {
                    return Object[].class;
                }
                else {
                    mappedResultType = ((NativeSQLQueryConstructorReturn) queryReturn).getTargetClass();
                }
            }
        }
        return mappedResultType;
    }
}
