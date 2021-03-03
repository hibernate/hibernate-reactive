/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.annotations.NaturalId;
import org.hibernate.cfg.Configuration;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

import static org.hibernate.reactive.common.Identifier.value;

public class NaturalIdTest extends BaseReactiveTest {

    @Override
    protected Configuration constructConfiguration() {
        Configuration configuration = super.constructConfiguration();
        configuration.addAnnotatedClass(Thing.class);
        return configuration;
    }

    @Test
    public void test(TestContext context) {
        Thing thing1 = new Thing();
        thing1.naturalKey = "abc123";
        thing1.version = 1;
        Thing thing2 = new Thing();
        thing2.naturalKey = "abc123";
        thing2.version = 2;
        test( context,
                getSessionFactory()
                        .withSession( session -> session.persist(thing1, thing2).thenCompose( v -> session.flush() ) )
                        .thenCompose( v -> getSessionFactory().withSession(
                                session -> session.find( Thing.class,
                                        value(Thing.class, "naturalKey", "abc123"),
                                        value(Thing.class, "version", 1)
                                )
                        ) )
                        .thenAccept( t -> {
                            context.assertNotNull(t);
                            context.assertEquals(thing1.id, t.id);
                        } )
                        .thenCompose( v -> getSessionFactory().withSession(
                                session -> session.find( Thing.class,
                                        value(Thing.class, "naturalKey", "abc123"),
                                        value(Thing.class, "version", 3)
                                )
                        ) )
                        .thenAccept(context::assertNull)
        );
    }

    @Entity
    static class Thing {
        @Id @GeneratedValue long id;
        @NaturalId String naturalKey;
        @NaturalId int version;
    }
}
