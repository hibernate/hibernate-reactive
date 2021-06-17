/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
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

import java.util.HashMap;
import java.util.Map;

public class TestingRegistryRule extends ExternalResource {

    private Vertx vertx;
    private Registry registry;

    @Override
    protected void before() {
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

    public <T> void addService (Class<T> type, T instance) {
        registry.add( type, instance );
    }

    //TODO extract into its own class and evolve as necessary?
    private static class Registry implements ServiceRegistryImplementor {

        private final Map<Class<?>, Object> services = new HashMap<>();

        public Registry(Vertx vertx) {
            add( VertxInstance.class, new ProvidedVertxInstance( vertx ) );
            add( JdbcEnvironment.class, new JdbcEnvironment() {
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
            } );
        }

        <T> void add(Class<T> type, T instance) {
            services.put( type, instance );
        }

        @Override
        public <R extends Service> ServiceBinding<R> locateServiceBinding(Class<R> serviceRole) {
            return null;
        }

        @Override
        public void destroy() {}

        @Override
        public void registerChild(ServiceRegistryImplementor child) {}

        @Override
        public void deRegisterChild(ServiceRegistryImplementor child) {}

        @Override
        public ServiceRegistry getParentServiceRegistry() {
            return null;
        }

        @Override @SuppressWarnings("unchecked")
        public <R extends Service> R getService(Class<R> serviceRole) {
            Object service = services.get(serviceRole);
            if ( service != null ) {
                return (R) service;
            }
            else {
                throw new IllegalArgumentException( "This is a mock service - need to explicitly handle any service we might need during testing" );
            }
        }
    }

}
