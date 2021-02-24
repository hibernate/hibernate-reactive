/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.Configuration;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;

public class JoinedSubclassIdentityTest extends BaseReactiveTest {

    @Override
    protected Configuration constructConfiguration() {
        Configuration configuration = super.constructConfiguration();
        configuration.addAnnotatedClass(GeneratedWithIdentityParent.class);
        configuration.addAnnotatedClass(GeneratedWithIdentity.class);
        return configuration;
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
