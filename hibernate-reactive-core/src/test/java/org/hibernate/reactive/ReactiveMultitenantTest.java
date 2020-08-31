/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.LockMode;
import org.hibernate.MultiTenancyStrategy;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.pool.impl.SqlClientPool;
import org.hibernate.reactive.pool.impl.SqlClientPoolInitiator;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Version;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

public class ReactiveMultitenantTest extends BaseReactiveTest {

    @Override
    protected void addServices(StandardServiceRegistryBuilder builder) {
        builder.addInitiator( new SqlClientPoolInitiator() {
            @Override
            public ReactiveConnectionPool initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
                return new SqlClientPool() {
                    @Override
                    public CompletionStage<ReactiveConnection> getConnection(String tenantId) {
                        return getConnection();
                    }
                };
            }
        } );
    }

    @Override
    protected Configuration constructConfiguration() {
        Configuration configuration = super.constructConfiguration();
        configuration.addAnnotatedClass( GuineaPig.class );
        configuration.setProperty( Environment.MULTI_TENANT, MultiTenancyStrategy.DATABASE.name() );
        configuration.setProperty( Environment.MULTI_TENANT_IDENTIFIER_RESOLVER, MyCurrentTenantIdentifierResolver.class.getName() );
        return configuration;
    }

    @Test
    public void reactivePersistFindDelete(TestContext context) {
        final GuineaPig guineaPig = new GuineaPig( 5, "Aloi" );
        Stage.Session session = getSessionFactory().openSession();
        test(
                context,
                session.persist( guineaPig )
                        .thenCompose( v -> session.flush() )
                        .thenAccept( v -> session.detach(guineaPig) )
                        .thenAccept( v -> context.assertFalse( session.contains(guineaPig) ) )
                        .thenCompose( v -> session.find( GuineaPig.class, guineaPig.getId() ) )
                        .thenAccept( actualPig -> {
                            assertThatPigsAreEqual( context, guineaPig, actualPig );
                            context.assertTrue( session.contains( actualPig ) );
                            context.assertFalse( session.contains( guineaPig ) );
                            context.assertEquals( LockMode.READ, session.getLockMode( actualPig ) );
                            session.detach( actualPig );
                            context.assertFalse( session.contains( actualPig ) );
                        } )
                        .thenCompose( v -> session.find( GuineaPig.class, guineaPig.getId() ) )
                        .thenCompose( pig -> session.remove(pig) )
                        .thenCompose( v -> session.flush() )
                        .whenComplete( (v, err) -> session.close() )
        );
    }

    private void assertThatPigsAreEqual(TestContext context, GuineaPig expected, GuineaPig actual) {
        context.assertNotNull( actual );
        context.assertEquals( expected.getId(), actual.getId() );
        context.assertEquals( expected.getName(), actual.getName() );
    }

    @Entity(name="GuineaPig")
    @Table(name="Pig")
    public static class GuineaPig {
        @Id
        private Integer id;
        private String name;
        @Version
        private int version;

        public GuineaPig() {
        }

        public GuineaPig(Integer id, String name) {
            this.id = id;
            this.name = name;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return id + ": " + name;
        }

        @Override
        public boolean equals(Object o) {
            if ( this == o ) {
                return true;
            }
            if ( o == null || getClass() != o.getClass() ) {
                return false;
            }
            GuineaPig guineaPig = (GuineaPig) o;
            return Objects.equals( name, guineaPig.name );
        }

        @Override
        public int hashCode() {
            return Objects.hash( name );
        }
    }
}
