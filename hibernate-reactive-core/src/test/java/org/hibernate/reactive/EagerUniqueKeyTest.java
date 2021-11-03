/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.Hibernate;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.hibernate.cfg.Configuration;
import org.junit.Test;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import java.io.Serializable;

public class EagerUniqueKeyTest extends BaseReactiveTest {
    protected Configuration constructConfiguration() {
        Configuration configuration = super.constructConfiguration();
        configuration.addAnnotatedClass(Foo.class);
        configuration.addAnnotatedClass(Bar.class);
        return configuration;
    }

    @Test
    public void testFindJoin(TestContext context) {
        Foo foo = new Foo(new Bar("unique"));
        test(context, getSessionFactory()
                .withTransaction( (session, transaction)
                        -> session.persist(foo)
                        .thenCompose( v -> session.flush() )
                        .thenAccept( v -> session.clear() )
                        .thenCompose( v -> session.find(Foo.class, foo.id) )
                        .thenAccept( result -> {
                            context.assertTrue( Hibernate.isInitialized(result.bar) );
                            context.assertEquals("unique", result.bar.key );
                        } )
                ) );
    }


    @Test
    public void testMergeDetached(TestContext context) {
        Bar bar = new Bar("unique2");
        test(context, getSessionFactory()
                .withTransaction( (session, tx) -> session.persist(bar) )
                .thenCompose( i -> getSessionFactory().withTransaction( (session, transaction)
                        -> session.merge( new Foo(bar) ) ) )
                .thenCompose( result -> getSessionFactory().withTransaction( (session, transaction)
                        -> session.fetch(result.bar).thenAccept( b -> context.assertEquals("unique2", b.key ) )
                ) ) );
    }

    @Test
    public void testMergeReference(TestContext context) {
        Bar bar = new Bar("unique3");
        test(context, getSessionFactory()
                .withTransaction( (session, tx) -> session.persist(bar) )
                .thenCompose( i -> getSessionFactory().withTransaction( (session, transaction)
                        -> session.merge( new Foo( session.getReference(Bar.class, bar.id) ) ) ) )
                .thenCompose( result -> getSessionFactory().withTransaction( (session, transaction)
                        -> session.fetch(result.bar).thenAccept( b -> context.assertEquals("unique3", b.key ) )
                ) ) );
    }

    @Entity(name="Foo")
    static class Foo {
        Foo(Bar bar) {
            this.bar = bar;
        }
        Foo(){}
        @GeneratedValue @Id
        long id;
        @ManyToOne(cascade = CascadeType.PERSIST, fetch = FetchType.EAGER)
        @Fetch(FetchMode.JOIN)
        @JoinColumn(name="bar_key", referencedColumnName = "nat_key")
        Bar bar;
    }

    @Entity(name="Bar")
    static class Bar implements Serializable {
        Bar(String key) {
            this.key = key;
        }
        Bar(){}
        @GeneratedValue @Id
        long id;
        @Column(name="nat_key", unique = true)
        String key;
    }
}
