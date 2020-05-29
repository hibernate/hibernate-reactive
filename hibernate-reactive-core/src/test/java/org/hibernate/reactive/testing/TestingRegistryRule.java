/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.testing;

import io.vertx.core.Vertx;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MySQL8Dialect;
import org.hibernate.engine.jdbc.env.spi.ExtractedDatabaseMetaData;
import org.hibernate.engine.jdbc.env.spi.IdentifierHelper;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.jdbc.env.spi.LobCreatorBuilder;
import org.hibernate.engine.jdbc.env.spi.NameQualifierSupport;
import org.hibernate.engine.jdbc.env.spi.QualifiedObjectNameFormatter;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.engine.jdbc.spi.TypeInfo;
import org.hibernate.reactive.vertx.VertxInstance;
import org.hibernate.reactive.vertx.impl.ProvidedVertxInstance;
import org.hibernate.service.Service;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.spi.ServiceBinding;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.junit.rules.ExternalResource;

public class TestingRegistryRule extends ExternalResource {

    private Vertx vertx;
    private Registry registry;

    @Override
    protected void before() throws Throwable {
        this.vertx = Vertx.vertx();
        this.registry = new Registry( vertx );
    }

    @Override
    protected void after() {
        if (vertx != null) {
            vertx.close();
        }
        this.registry = null;
    }

    public ServiceRegistryImplementor getServiceRegistry() {
        return registry;
    }

    //TODO extract into its own class and evolve as necessary?
    private static class Registry implements ServiceRegistryImplementor {

        private final ProvidedVertxInstance vertxService;

        public Registry(Vertx vertx) {
            this.vertxService = new ProvidedVertxInstance( vertx );
        }

        @Override
        public <R extends Service> ServiceBinding<R> locateServiceBinding(Class<R> serviceRole) {
            return null;
        }

        @Override
        public void destroy() {

        }

        @Override
        public void registerChild(ServiceRegistryImplementor child) {

        }

        @Override
        public void deRegisterChild(ServiceRegistryImplementor child) {

        }

        @Override
        public ServiceRegistry getParentServiceRegistry() {
            return null;
        }

        @Override @SuppressWarnings("unchecked")
        public <R extends Service> R getService(Class<R> serviceRole) {
            if ( serviceRole == VertxInstance.class ) {
                return (R) vertxService;
            }
            else if ( serviceRole == JdbcEnvironment.class ) {
                return (R) new JdbcEnvironment() {
                    @Override
                    public Dialect getDialect() {
                        return new MySQL8Dialect();
                    }

                    @Override
                    public ExtractedDatabaseMetaData getExtractedDatabaseMetaData() {
                        return null;
                    }

                    @Override
                    public Identifier getCurrentCatalog() {
                        return null;
                    }

                    @Override
                    public Identifier getCurrentSchema() {
                        return null;
                    }

                    @Override
                    public QualifiedObjectNameFormatter getQualifiedObjectNameFormatter() {
                        return null;
                    }

                    @Override
                    public IdentifierHelper getIdentifierHelper() {
                        return null;
                    }

                    @Override
                    public NameQualifierSupport getNameQualifierSupport() {
                        return null;
                    }

                    @Override
                    public SqlExceptionHelper getSqlExceptionHelper() {
                        return null;
                    }

                    @Override
                    public LobCreatorBuilder getLobCreatorBuilder() {
                        return null;
                    }

                    @Override
                    public TypeInfo getTypeInfoForJdbcCode(int jdbcTypeCode) {
                        return null;
                    }
                };
            }
            else {
                throw new IllegalArgumentException( "This is a mock service - need to explicitly handle any service we might need during testing" );
            }
        }
    }

}
