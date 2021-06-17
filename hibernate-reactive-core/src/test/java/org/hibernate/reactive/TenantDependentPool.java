/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.net.URI;
import java.util.Map;
import java.util.stream.Collectors;

import org.hibernate.reactive.MyCurrentTenantIdentifierResolver.Tenant;
import org.hibernate.reactive.pool.impl.DefaultSqlClientPool;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnectOptions;
import io.vertx.sqlclient.SqlConnection;

import static java.util.Arrays.stream;

/**
 * A {@link org.hibernate.reactive.pool.ReactiveConnectionPool} that creates different
 * {@link Pool} for each possible {@link Tenant}. Each tenant will connect to a different
 * database.
 * <p>
 * WARNING: This class is only for testing and it's not supposed to be used in production.
 * </p>
 */
public class TenantDependentPool extends DefaultSqlClientPool {

    private MultiplePools pools;

    @Override
    protected Pool getTenantPool(String tenantId) {
        return pools.getTenantPool( Tenant.valueOf( tenantId ) );
    }

    @Override
    protected Pool createPool(URI uri, SqlConnectOptions connectOptions, PoolOptions poolOptions, Vertx vertx) {
        Map<Tenant, Pool> poolMap = stream( Tenant.values() )
                .collect( Collectors.toMap(
                        tenant -> tenant,
                        tenant -> createPool( uri, connectOptions, poolOptions, vertx, tenant )
                ) );
        pools = new MultiplePools( poolMap );
        return pools;
    }

    private Pool createPool(URI uri, SqlConnectOptions connectOptions, PoolOptions poolOptions, Vertx vertx, Tenant tenant) {
        return super.createPool( changeDbName( uri, tenant ), changeDbName( connectOptions, tenant ), poolOptions, vertx );
    }

    /**
     * Replace the database in the connection string for PostgreSQL.
     * <p>
     * PostgreSQL connection string example:
     *
     * <blockquote>
     * {@code postgresql://localhost:5432/hreact?loggerLevel=OFF&user=hreact&password=hreact}
     * </blockquote>
     * </p>
     */
    private static URI changeDbName(URI uri, Tenant tenant) {
        String uriAsString = uri.toString()
                .replaceAll( "/[\\w\\d]+\\?", "/" + tenant.getDbName() + "?" );
        return URI.create( uriAsString );
    }

    /**
     * Returns a new {@link SqlConnectOptions} with the correct database name for the tenant identifier.
     */
    private SqlConnectOptions changeDbName(SqlConnectOptions connectOptions, Tenant tenantId) {
        SqlConnectOptions newOptions = new SqlConnectOptions( connectOptions );
        newOptions.setDatabase( tenantId.getDbName() );
        return newOptions;
    }

    private static class MultiplePools implements Pool {
        private final Map<Tenant, Pool> poolMap;
        private final Tenant defaultTenantId;

        private MultiplePools(Map<Tenant, Pool> poolMap) {
            this.poolMap = poolMap;
            this.defaultTenantId = Tenant.DEFAULT;
        }

        public Pool getTenantPool(Tenant tenantId) {
            return poolMap.get( tenantId );
        }

        @Override
        public void getConnection(Handler<AsyncResult<SqlConnection>> handler) {
            poolMap.get( defaultTenantId ).getConnection( handler );
        }

        @Override
        public Future<SqlConnection> getConnection() {
            return poolMap.get( defaultTenantId ).getConnection();
        }

        @Override
        public Query<RowSet<Row>> query(String sql) {
            return poolMap.get( defaultTenantId ).query( sql );
        }

        @Override
        public PreparedQuery<RowSet<Row>> preparedQuery(String sql) {
            return poolMap.get( defaultTenantId ).preparedQuery( sql );
        }

        @Override
        public void close(Handler<AsyncResult<Void>> handler) {
            poolMap.forEach( (tenant, pool) -> pool.close( handler ) );
        }

        @Override
        public Future<Void> close() {
            Future<Void> close = Future.succeededFuture();
            for ( Pool pool : poolMap.values() ) {
                close = close.compose( unused -> pool.close() );
            }
            return close;
        }
    }
}
