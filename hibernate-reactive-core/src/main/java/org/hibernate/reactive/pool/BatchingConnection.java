/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool;

import org.hibernate.reactive.util.impl.CompletionStages;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * A {@link ReactiveConnection} that automatically performs batching
 * of insert, update, and delete statements, relieving the client
 * persister code of the responsibility to manage batching. Actual
 * SQL statements are delegated to a given {@link ReactiveConnection}
 * which only supports explicit batching using {@link #update(String, List)}.
 * <p>
 * Note that in Hibernate core, the responsibilities of this class
 * are handled by {@link org.hibernate.engine.jdbc.spi.JdbcCoordinator}
 * and the {@link org.hibernate.engine.jdbc.batch.spi.Batch} interface.
 * However, the model used there is not easily adaptable to the reactive
 * paradigm.
 *
 * @author Gavin King
 */
public class BatchingConnection implements ReactiveConnection {

    private final ReactiveConnection delegate;
    private final int batchSize;

    private String batchedSql;
    private Expectation batchedExpectation;
    private List<Object[]> batchParamValues;

    public BatchingConnection(ReactiveConnection delegate, int batchSize) {
        this.delegate = delegate;
        this.batchSize = batchSize;
    }

    @Override
    public CompletionStage<Void> executeBatch() {
        if ( !hasBatch() ) {
            return CompletionStages.voidFuture();
        }
        else {
            String sql = batchedSql;
            Expectation expectation = batchedExpectation;
            List<Object[]> paramValues = batchParamValues;
            batchedSql = null;
            batchParamValues = null;
            batchedExpectation = null;

            if ( paramValues.size()==1 ) {
                return update( sql, paramValues.get(0) )
                        .thenAccept( rowCount -> expectation.verifyOutcome( rowCount, -1, sql ) );
            }
            else {
                return update( sql, paramValues )
                        .thenAccept( rowCounts -> {
                            for ( int i=0; i<rowCounts.length; i++ ) {
                                expectation.verifyOutcome( rowCounts[i], i, sql );
                            }
                        } );
                }
        }
    }

    public CompletionStage<Void> update(String sql, Object[] paramValues,
                                        boolean allowBatching, Expectation expectation) {
        if ( allowBatching && batchSize>0 ) {
            if ( !hasBatch() ) {
                newBatch( sql, paramValues, expectation );
                return CompletionStages.voidFuture();
            }
            else {
                if ( batchedSql.equals(sql) && batchParamValues.size()<batchSize ) {
                    batchParamValues.add(paramValues);
                    return CompletionStages.voidFuture();
                }
                else {
                    CompletionStage<Void> lastBatch = executeBatch();
                    newBatch( sql, paramValues, expectation );
                    return lastBatch;
                }
            }
        }
        else {
            return delegate.update( sql, paramValues, false, expectation );
        }
    }

    private void newBatch(String sql, Object[] paramValues, Expectation expectation) {
        batchedSql = sql;
        batchedExpectation = expectation;
        batchParamValues = new ArrayList<>();
        batchParamValues.add(paramValues);
    }

    private boolean hasBatch() {
        return batchedSql != null;
    }

    public CompletionStage<Void> execute(String sql) {
        return delegate.execute(sql);
    }

    public CompletionStage<Integer> update(String sql) {
         return hasBatch() ?
                 executeBatch().thenCompose( v -> delegate.update(sql) ) :
                 delegate.update(sql);
    }

    @Override
    public CompletionStage<Integer> update(String sql, Object[] paramValues) {
        return hasBatch() ?
                executeBatch().thenCompose( v -> delegate.update(sql, paramValues) ) :
                delegate.update(sql, paramValues);
    }

    public CompletionStage<int[]> update(String sql, List<Object[]> paramValues) {
        return hasBatch() ?
                executeBatch().thenCompose( v -> delegate.update(sql, paramValues) ) :
                delegate.update(sql, paramValues);
    }

    public CompletionStage<Long> updateReturning(String sql, Object[] paramValues) {
        return hasBatch() ?
                executeBatch().thenCompose( v -> delegate.updateReturning(sql, paramValues) ) :
                delegate.updateReturning(sql, paramValues);
    }

    public CompletionStage<ReactiveConnection.Result> select(String sql) {
        return hasBatch() ?
                executeBatch().thenCompose( v -> delegate.select(sql) ) :
                delegate.select(sql);
    }

    public CompletionStage<ReactiveConnection.Result> select(String sql, Object[] paramValues) {
        return hasBatch() ?
                executeBatch().thenCompose( v -> delegate.select(sql, paramValues) ) :
                delegate.select(sql, paramValues);
    }

    public CompletionStage<ResultSet> selectJdbc(String sql, Object[] paramValues) {
        return hasBatch() ?
                executeBatch().thenCompose( v -> delegate.selectJdbc(sql, paramValues) ) :
                delegate.selectJdbc(sql, paramValues);
    }

    public CompletionStage<Long> selectLong(String sql, Object[] paramValues) {
        return hasBatch() ?
                executeBatch().thenCompose( v -> delegate.selectLong(sql, paramValues) ) :
                delegate.selectLong(sql, paramValues);
    }

    public CompletionStage<Void> beginTransaction() {
        return delegate.beginTransaction();
    }

    public CompletionStage<Void> commitTransaction() {
        return delegate.commitTransaction();
    }

    public CompletionStage<Void> rollbackTransaction() {
        return delegate.rollbackTransaction();
    }

    public void close() {
        delegate.close();
    }
}
