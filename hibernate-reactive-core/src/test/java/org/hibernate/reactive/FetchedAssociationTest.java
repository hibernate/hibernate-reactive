/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.provider.Settings;

import org.junit.After;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

public class FetchedAssociationTest extends BaseReactiveTest {

    @Override
    protected Configuration constructConfiguration() {
        Configuration configuration = super.constructConfiguration();
        configuration.addAnnotatedClass( Parent.class );
        configuration.addAnnotatedClass( Child.class );
        configuration.setProperty( Settings.SHOW_SQL, "true");
        return configuration;
    }

    @After
    public void cleanDb(TestContext context) {
        test( context, deleteEntities( Child.class, Parent.class ) );
    }

    @Test
    public void testWithMutiny(TestContext context) {
        test( context, getMutinySessionFactory()
                .withTransaction( s -> {
                    final Parent parent = new Parent();
                    parent.setName( "Parent" );
                    return s.persist( parent );
                } )
                .call( () -> getMutinySessionFactory()
                        .withTransaction( s -> s
                                .createQuery( "From Parent", Parent.class )
                                .getSingleResult()
                                .call( parent -> {
                                    Child child = new Child();
                                    child.setParent( parent );
                                    parent.getChildren().add( child );
                                    return s.persist( child );
                                } )
                        )
                )
        );
    }

    @Entity(name = "Parent")
    @Table(name = "PARENT")
    public static class Parent {
        @Id
        @GeneratedValue
        private Long id;

        private String name;

        @OneToMany(cascade = CascadeType.PERSIST, fetch = FetchType.LAZY, mappedBy = "parent")
        private List<Child> children = new ArrayList<>();

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<Child> getChildren() {
            return children;
        }
    }

    @Entity(name = "Child")
    @Table(name = "CHILD")
    public static class Child {
        @Id
        @GeneratedValue
        private Long id;

        private String name;

        @ManyToOne
        @JoinColumn(name = "lazy_parent_id")
        private Parent parent;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public Parent getParent() {
            return parent;
        }

        public void setParent(Parent parent) {
            this.parent = parent;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
