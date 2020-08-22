/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.bulk;

import org.hibernate.param.ParameterSpecification;

/**
 * A list of SQL statements to be executed as a single logical unit.
 * May include both DDL and DML statements.
 *
 * @author Gavin King
 */
public interface StatementsWithParameters {

    /**
     * The SQL statements to be executed.
     */
    String[] getSqlStatements();

    /**
     * The parameters of the corresponding SQL statements.
     */
    ParameterSpecification[][] getParameterSpecifications();

    /**
     * Is the given statement executed inside the current transaction?
     *
     * @return true by default
     */
    default boolean isTransactionalStatement(String statement) {
        return true;
    }

    /**
     * Should the result of this statement contribute to the running
     * updated row count?
     *
     * @return false for DDL statements by default
     */
    default boolean isSchemaDefinitionStatement(String statement) {
        return statement.startsWith("create ")
            || statement.startsWith("drop ");
    }
}
