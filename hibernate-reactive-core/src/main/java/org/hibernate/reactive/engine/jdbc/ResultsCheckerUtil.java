/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.jdbc;

import java.sql.SQLException;
import org.hibernate.engine.jdbc.mutation.OperationResultChecker;
import org.hibernate.engine.jdbc.mutation.group.PreparedStatementDetails;
import org.hibernate.engine.jdbc.mutation.internal.ModelMutationHelper;
import org.hibernate.engine.spi.SharedSessionContractImplementor;

public final class ResultsCheckerUtil {

    private ResultsCheckerUtil() {
    }


    public static void checkResults(
            SharedSessionContractImplementor session,
            PreparedStatementDetails statementDetails,
            OperationResultChecker resultChecker,
            Integer affectedRowCount, int batchPosition) {
        try {
            ModelMutationHelper.checkResults( resultChecker, statementDetails, affectedRowCount, batchPosition);
        }
        catch (SQLException e) {
            throw session.getJdbcServices().getSqlExceptionHelper()
                    .convert(
                    e,
                    String.format(
                            "Unable to execute mutation PreparedStatement against table `%s`",
                            statementDetails.getMutatingTableDetails().getTableName()
                    ),
                    statementDetails.getSqlString()
            );
        }
    }
}
