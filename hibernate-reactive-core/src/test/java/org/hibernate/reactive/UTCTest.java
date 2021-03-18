/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class UTCTest extends BaseReactiveTest {
    @Override
    protected Configuration constructConfiguration() {
        Configuration configuration = super.constructConfiguration();
        configuration.setProperty(AvailableSettings.JDBC_TIME_ZONE, "UTC");
        configuration.addAnnotatedClass(Thing.class);
        return configuration;
    }

    @Test
    public void test(TestContext context) {
        Thing thing = new Thing();
        thing.dateTime = OffsetDateTime.of(2021, 3, 25, 12, 30, 0, 0, ZoneOffset.ofHours(5));
        test(context, getMutinySessionFactory()
                .withSession(session -> session.persist(thing).call(session::flush))
                .chain( () -> getMutinySessionFactory()
                        .withSession(session -> session.find(Thing.class, thing.id))
                        .invoke(t -> {
                            context.assertNotNull(t);
                            context.assertTrue( thing.dateTime.isEqual(t.dateTime) );
                        })
                )
                .chain( () -> getMutinySessionFactory()
                        .withSession(session -> session.createQuery("from ThingInUTC where dateTime=:dt")
                                .setParameter("dt", thing.dateTime).getSingleResult())
                        .invoke(context::assertNotNull)
                )
        );
    }

    @Entity(name="ThingInUTC")
    static class Thing {
        @Id
        @GeneratedValue
        long id;

        OffsetDateTime dateTime;
    }
}
