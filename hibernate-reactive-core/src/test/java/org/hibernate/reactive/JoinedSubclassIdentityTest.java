/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.Configuration;

import org.junit.After;
import org.junit.Test;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Inheritance;
import jakarta.persistence.InheritanceType;

public class JoinedSubclassIdentityTest extends BaseReactiveTest {

    @Override
    protected Configuration constructConfiguration() {
        Configuration configuration = super.constructConfiguration();
        configuration.addAnnotatedClass(GeneratedWithIdentityParent.class);
        configuration.addAnnotatedClass(GeneratedWithIdentity.class);
        return configuration;
    }

    @After
    public void cleanDb(TestContext context) {
        test( context, deleteEntities( "GeneratedWithIdentityParent", "GeneratedWithIdentity" ) );
    }

    @Test public void testParent(TestContext context) {
        test(context, getMutinySessionFactory().withSession(
                s -> s.persist( new GeneratedWithIdentityParent() )
                .chain( s::flush )
        ));
    }

    @Test public void testChild(TestContext context) {
        test(context, getMutinySessionFactory().withSession(
                s -> s.persist( new GeneratedWithIdentity() )
                        .chain( s::flush )
        ));
    }

    @Entity(name="GeneratedWithIdentityParent")
    @Inheritance(strategy = InheritanceType.JOINED)
    static class GeneratedWithIdentityParent {
        @Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        public Long id;

        public String firstname;
    }

    @Entity(name="GeneratedWithIdentity")
    static class GeneratedWithIdentity extends GeneratedWithIdentityParent {
        public String updatedBy;
    }
}
