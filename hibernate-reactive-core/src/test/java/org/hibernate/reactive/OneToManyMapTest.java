/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.Hibernate;
import org.hibernate.cfg.Configuration;
import org.junit.Test;

import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.util.HashMap;
import java.util.Map;

public class OneToManyMapTest extends BaseReactiveTest {

    @Override
    protected Configuration constructConfiguration() {
        Configuration configuration = super.constructConfiguration();
        configuration.addAnnotatedClass( Book.class );
        configuration.addAnnotatedClass( Author.class );
        return configuration;
    }

    @Test
    public void test(TestContext context) {
        Book book1 = new Book("Feersum Endjinn");
        Book book2 = new Book("Use of Weapons");
        Author author = new Author("Iain M Banks");
        author.books.put("a", book1);
        author.books.put("b", book2);

        test(context,
                getMutinySessionFactory()
                        .withTransaction( (session, transaction) -> session.persistAll(book1, book2, author) )
                        .chain( () -> getMutinySessionFactory()
                                .withTransaction( (session, transaction) -> session.find(Author.class, author.id)
                                        .invoke( a -> context.assertFalse( Hibernate.isInitialized(a.books) ) )
                                        .chain( a -> session.fetch(a.books) )
                                        .invoke( books -> {
                                            context.assertEquals( 2, books.size() );
                                            context.assertEquals( book1.title, books.get("a").title );
                                            context.assertEquals( book2.title, books.get("b").title );
                                        } )
                                )
                        )
                        .chain( () -> getMutinySessionFactory()
                                .withTransaction( (session, transaction) -> session.createQuery("select distinct a from Author a left join fetch a.books", Author.class )
                                        .getSingleResult()
                                        .invoke( a -> context.assertTrue( Hibernate.isInitialized(a.books) ) )
                                        .invoke( a -> {
                                            context.assertEquals( 2, a.books.size() );
                                            context.assertEquals( book1.title, a.books.get("a").title );
                                            context.assertEquals( book2.title, a.books.get("b").title );
                                        } )
                                )
                        )
        );
    }

    @Entity(name="Book")
    @Table(name="OTMMBook")
    static class Book {
        Book(String title) {
            this.title = title;
        }
        Book() {}
        @GeneratedValue @Id long id;

        @Basic(optional = false)
        String title;
    }

    @Entity(name="Author")
    @Table(name="OTMMAuthor")
    static class Author {
        Author(String name) {
            this.name = name;
        }
        public Author() {}
        @GeneratedValue @Id long id;

        @Basic(optional = false)
        String name;

        @OneToMany
        Map<String,Book> books = new HashMap<>();
    }
}
