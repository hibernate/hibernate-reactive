/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.persistence.Basic;
import javax.persistence.CollectionTable;
import javax.persistence.ElementCollection;
import javax.persistence.Embeddable;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OrderColumn;
import javax.persistence.Table;

import org.hibernate.Hibernate;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;
import org.assertj.core.api.Assertions;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;

public class OrderedEmbeddableCollectionTest extends BaseReactiveTest {

    @Rule // This exposes a strange bug in the DB2 client
    public DatabaseSelectionRule dbRule = DatabaseSelectionRule.skipTestsFor( DB2 );

    @Override
    protected Configuration constructConfiguration() {
        Configuration configuration = super.constructConfiguration();
        configuration.addAnnotatedClass( Book.class );
        configuration.addAnnotatedClass( Author.class );
        return configuration;
    }

    @After
    public void cleanDb(TestContext context) {
        test( context, deleteEntities( "Author" ) );
    }

    @Test
    public void test(TestContext context) {
        Book book1 = new Book("Feersum Endjinn");
        Book book2 = new Book("Use of Weapons");
        Author author = new Author("Iain M Banks");
        author.books.add(book1);
        author.books.add(book2);

        test(context,
                getMutinySessionFactory()
                        .withTransaction( (session, transaction) -> session.persistAll(author) )
                        .chain( () -> getMutinySessionFactory()
                                .withTransaction( (session, transaction) -> session.find(Author.class, author.id)
                                        .invoke( a -> context.assertFalse( Hibernate.isInitialized(a.books) ) )
                                        .chain( a -> session.fetch(a.books) )
                                        .invoke( books -> context.assertEquals( 2, books.size() ) )
                                )
                        )
                        .chain( () -> getMutinySessionFactory()
                                .withTransaction( (session, transaction) -> session.createQuery("select distinct a from Author a left join fetch a.books", Author.class )
                                        .getSingleResult()
                                        .invoke( a -> context.assertTrue( Hibernate.isInitialized(a.books) ) )
                                        .invoke( a -> context.assertEquals( 2, a.books.size() ) )
                                )
                        )
                        .chain( () -> getMutinySessionFactory()
                                .withTransaction( (session, transaction) -> session.find(Author.class, author.id)
                                        .chain( a -> session.fetch(a.books) )
                                        .invoke( books -> books.remove(0) )
                                )
                        )
                        .chain( () -> getMutinySessionFactory()
                                .withTransaction( (session, transaction) -> session.find(Author.class, author.id)
                                        .chain( a -> session.fetch(a.books) )
                                        .invoke( books -> {
                                            context.assertEquals( 1, books.size() );
                                            context.assertEquals( book2.title, books.get(0).title );
                                        } )
                                )
                        )
                        .chain( () -> getMutinySessionFactory()
                                .withTransaction( (session, transaction) -> session.find(Author.class, author.id)
                                        .chain( a -> session.fetch(a.books) )
                                        .invoke( books -> books.add(book1) )
                                )
                        )
                        .chain( () -> getMutinySessionFactory()
                                .withTransaction( (session, transaction) -> session.find(Author.class, author.id)
                                        .invoke( a -> context.assertFalse( Hibernate.isInitialized(a.books) ) )
                                        .chain( a -> session.fetch(a.books) )
                                        .invoke( books -> context.assertEquals( 2, books.size() ) )
                                )
                        )
                        .chain( () -> getMutinySessionFactory()
                                .withTransaction( (session, transaction) -> session.find(Author.class, author.id)
                                        .chain( a -> session.fetch(a.books) )
                                        .invoke( books -> books.remove(1) )
                                )
                        )
                        .chain( () -> getMutinySessionFactory()
                                .withTransaction( (session, transaction) -> session.find(Author.class, author.id)
                                        .chain( a -> session.fetch(a.books) )
                                        .invoke( books -> {
                                            context.assertEquals( 1, books.size() );
                                            context.assertEquals( book2.title, books.get(0).title );
                                        } )
                                )
                        )
                        .chain( () -> getMutinySessionFactory()
                                .withTransaction( (session, transaction) -> session.find(Author.class, author.id)
                                        .chain( a -> session.fetch(a.books) )
                                        .invoke( books -> books.add(book1) )
                                )
                        )
                        .chain( () -> getMutinySessionFactory()
                                .withTransaction( (session, transaction) -> session.find(Author.class, author.id)
                                        .invoke( a -> context.assertFalse( Hibernate.isInitialized(a.books) ) )
                                        .chain( a -> session.fetch(a.books) )
                                        .invoke( books -> context.assertEquals( 2, books.size() ) )
                                )
                        )
                        .chain( () -> getMutinySessionFactory()
                                .withTransaction( (session, transaction) -> session.find(Author.class, author.id)
                                        .chain( a -> session.fetch(a.books) )
                                        .invoke( books -> books.add( books.remove(0) ) )
                                )
                        )
                        .chain( () -> getMutinySessionFactory()
                                .withTransaction( (session, transaction) -> session.find(Author.class, author.id)
                                        .invoke( a -> context.assertFalse( Hibernate.isInitialized(a.books) ) )
                                        .chain( a -> session.fetch(a.books) )
                                        .invoke( books -> context.assertEquals( 2, books.size() ) )
                                )
                        )
                        .chain( () -> getMutinySessionFactory()
                                .withTransaction( (session, transaction) -> session.find(Author.class, author.id)
                                        .invoke( a -> a.books = null )
                                )
                        )
                        .chain( () -> getMutinySessionFactory()
                                .withTransaction( (session, transaction) -> session.find(Author.class, author.id)
                                        .chain( a -> session.fetch(a.books) )
                                        .invoke( books -> context.assertTrue( books.isEmpty() ) )
                                )
                        )
        );
    }

    @Test
    public void testMultipleRemovesFromCollection(TestContext context) {
        Book book1 = new Book("Feersum Endjinn");
        Book book2 = new Book("Use of Weapons");
        Book book3 = new Book( "Third Book");
        Book book4 = new Book( "Fourth Book");
        Book book5 = new Book( "Fifth Book");
        Author author = new Author("Iain M Banks");
        author.books.add(book1);
        author.books.add(book2);
        author.books.add(book3);
        author.books.add(book4);
        author.books.add(book5);

        test(context,
             getMutinySessionFactory()
                     .withTransaction( (session, transaction) -> session.persistAll(author) )
                     .chain( () -> getMutinySessionFactory()
                             .withTransaction( (session, transaction) -> session.find(Author.class, author.id)
                                     .invoke( a -> context.assertFalse( Hibernate.isInitialized(a.books) ) )
                                     .chain( a -> session.fetch(a.books) )
                                     .invoke( books -> context.assertEquals( 5, books.size() ) )
                             )
                     )
                     .chain( () -> getMutinySessionFactory()
                             .withTransaction( (session, transaction) -> session.createQuery("select distinct a from Author a left join fetch a.books", Author.class )
                                     .getSingleResult()
                                     .invoke( a -> context.assertTrue( Hibernate.isInitialized(a.books) ) )
                                     .invoke( a -> context.assertEquals( 5, a.books.size() ) )
                             )
                     )
                     .chain( () -> getMutinySessionFactory()
                             .withTransaction( (session, transaction) -> session.find(Author.class, author.id)
                                     .chain( a -> session.fetch(a.books) )
                                     .invoke( books -> {
                                         books.remove( 1 ); // Remove book2
                                         books.remove( 1 ); // Now, remove book3
                                     } )
                             )
                     )
                     .chain( () -> getMutinySessionFactory()
                             .withTransaction( (session, transaction) -> session.find(Author.class, author.id)
                                     .chain( a -> session.fetch(a.books) )
                                     .invoke( books -> {
                                         context.assertEquals( 3, books.size() );
                                         context.assertEquals( book4.title, books.get(1).title );
                                         Assertions.assertThat( books ).containsExactly( book1, book4, book5 );
                                     } )
                             )
                     )
        );
    }

    @Embeddable
    static class Book {
        Book(String title) {
            this.title = title;
        }
        Book() {}
        @Basic(optional = false)
        String title;
        @Override
        public boolean equals(Object o) {
            if ( this == o ) {
                return true;
            }
            if ( o == null || getClass() != o.getClass() ) {
                return false;
            }
            Book book = (Book) o;
            return Objects.equals( title, book.title );
        }
        @Override
        public int hashCode() {
            return Objects.hash( title );
        }
    }

    @Entity(name="Author")
    @Table(name="ECAuthor")
    static class Author {
        Author(String name) {
            this.name = name;
        }
        public Author() {}
        @GeneratedValue @Id long id;

        @Basic(optional = false)
        String name;

        @ElementCollection
        @CollectionTable(name="ECBook")
        @OrderColumn(name="list_index")
        List<Book> books = new ArrayList<>();
    }
}
