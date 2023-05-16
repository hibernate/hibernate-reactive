/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.example;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.*;
import jakarta.persistence.criteria.*;
import org.hibernate.reactive.BaseReactiveTest;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static jakarta.persistence.FetchType.LAZY;
import static java.lang.System.out;
import static java.time.Month.*;

public class MutinyMain extends BaseReactiveTest {

    @Override
    protected Collection<Class<?>> annotatedEntities() {
        return List.of( Book.class, Author.class );
    }

    @Test
    public void testWithInsert(VertxTestContext context) {
        out.println( "== Mutiny API Example ==" );

        // obtain a factory for reactive sessions based on the
        // standard JPA configuration properties specified in
        // resources/META-INF/persistence.xml

        // define some test data
        Author author1 = new Author("Iain M. Banks");
        Author author2 = new Author("Neal Stephenson");
        Book book1 = new Book("1-85723-235-6", "Feersum Endjinn", author1, LocalDate.of(1994, JANUARY, 1));
        Book book2 = new Book("0-380-97346-4", "Cryptonomicon", author2, LocalDate.of(1999, MAY, 1));
        Book book3 = new Book("0-553-08853-X", "Snow Crash", author2, LocalDate.of(1992, JUNE, 1));
        author1.getBooks().add(book1);
        author2.getBooks().add(book2);
        author2.getBooks().add(book3);

        CriteriaBuilder builder = getSessionFactory().getCriteriaBuilder();
        CriteriaQuery<Book> query = builder.createQuery( Book.class );
        Root<Book> b = query.from( Book.class );
        b.fetch( "author" );
        ParameterExpression<String> t = builder.parameter( String.class );
        query.where( builder.equal( b.get( "title" ), t ) );
        query.orderBy( builder.asc( b.get( "isbn" ) ) );

        CriteriaUpdate<Book> update = builder.createCriteriaUpdate( Book.class );
        b = update.from( Book.class );
        update.where( builder.equal( b.get( "title" ), t ) );
        update.set( b.get( "title" ), "XXX" );

        test(
                context,
                getMutinySessionFactory().withStatelessSession(session ->
                                session.withTransaction(tx -> session.insertAll(author1, author2, book1, book2, book3)
                                ))
                        .chain(() ->
                                getMutinySessionFactory().withStatelessSession(
                                        session -> session.get(Author.class, author1.getId())
                                                .invoke(author -> out.println("Author.name: " + author.getName())))
                        )
                        .chain(() ->
                                getMutinySessionFactory().withStatelessSession(
                                        session -> session.get(Book.class, book3.getId())
                                                .invoke(book -> out.println("Book.title: " + book.getTitle())))
                        ).chain(() ->
                                getMutinySessionFactory().withStatelessSession(
                                        session -> session.createNativeQuery(
                                                        "select book.title, author.name from books book join authors author on book.author_id = author.id order by book.title desc"
//                                                        ,Object.class
                                                )
                                                .getResultList()
                                                .invoke( rows -> {
                                                    List topArray = rows;

                                                    for( Object row : topArray ) {
                                                        final Object[] theRowArray = (Object[])row;
                                                        out.printf("%s : (%s)\n", theRowArray[0], theRowArray[1]);
                                                    }

//                                                    rows.forEach(
//                                                            row -> out.printf("%s (%s)\n", row)
//                                                    )
                                                } )
                                )
                        )
        );
    }

    @Test
    public void test(VertxTestContext context) {
        out.println( "== Mutiny API Example ==" );

        // obtain a factory for reactive sessions based on the
        // standard JPA configuration properties specified in
        // resources/META-INF/persistence.xml

        // define some test data
        Author author1 = new Author("Iain M. Banks");
        Author author2 = new Author("Neal Stephenson");
        Book book1 = new Book("1-85723-235-6", "Feersum Endjinn", author1, LocalDate.of(1994, JANUARY, 1));
        Book book2 = new Book("0-380-97346-4", "Cryptonomicon", author2, LocalDate.of(1999, MAY, 1));
        Book book3 = new Book("0-553-08853-X", "Snow Crash", author2, LocalDate.of(1992, JUNE, 1));
        author1.getBooks().add(book1);
        author2.getBooks().add(book2);
        author2.getBooks().add(book3);

        CriteriaBuilder builder = getSessionFactory().getCriteriaBuilder();
        CriteriaQuery<Book> query = builder.createQuery( Book.class );
        Root<Book> b = query.from( Book.class );
        b.fetch( "author" );
        ParameterExpression<String> t = builder.parameter( String.class );
        query.where( builder.equal( b.get( "title" ), t ) );
        query.orderBy( builder.asc( b.get( "isbn" ) ) );

        CriteriaUpdate<Book> update = builder.createCriteriaUpdate( Book.class );
        b = update.from( Book.class );
        update.where( builder.equal( b.get( "title" ), t ) );
        update.set( b.get( "title" ), "XXX" );

        test(
                context,
                getMutinySessionFactory().withTransaction( (session, tx) -> session
                        .persistAll( author1, author2 )
                        .call( session::flush )
                        .chain( () -> session.find( Book.class, book1.getId() )
                        .invoke( singleBook -> out.println( book1.getTitle() + " is a great book!" ) ) )
                ).chain( () ->
                      getMutinySessionFactory().withSession(
                              session -> session.find( Author.class, author1.getId(), author2.getId() )
                                      .invoke( authors -> authors.forEach( author -> out.println("Author.name: " + author.getName() ) ) )
                      )
                )

                /*
                		test(
				context,
				openSession()
						.thenCompose( session -> session.persist( author1, author2 )
								.thenCompose( v -> session.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery( query )
								.setParameter( t, "Snow Crash" )
								.getResultList() )
						.thenAccept( books -> {
							assertEquals( 1, books.size() );
							books.forEach( book -> {
								assertNotNull( book.id );
								assertNotNull( book.title );
								assertNotNull( book.isbn );
								assertEquals( "Snow Crash", book.title );
							} );
						} )

						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery( update )
								.setParameter( t, "Snow Crash" )
								.executeUpdate() )
						.thenCompose( v -> openSession() )
						.thenCompose( session -> session.createQuery( delete )
								.setParameter( t, "Snow Crash" )
								.executeUpdate() )
		);
                 */
        );
    }

    /*
        try {
            // obtain a reactive session
            factory.withTransaction(
                            // persist the Authors with their Books in a transaction
                            (session, tx) -> session.persistAll( author1, author2 )
                    )
                    // wait for it to finish
                    .await().indefinitely();

            factory.withSession(
                            // retrieve a Book
                            session -> session.find( Book.class, book1.getId() )
                                    // print its title
                                    .invoke( book -> out.println( book.getTitle() + " is a great book!" ) )
                    )
                    .await().indefinitely();

            factory.withSession(
                            // retrieve both Authors at once
                            session -> session.find( Author.class, author1.getId(), author2.getId() )
                                    .invoke( authors -> authors.forEach( author -> out.println( author.getName() ) ) )
                    )
                    .await().indefinitely();

            factory.withSession(
                            // retrieve an Author
                            session -> session.find( Author.class, author2.getId() )
                                    // lazily fetch their books
                                    .chain( author -> fetch( author.getBooks() )
                                            // print some info
                                            .invoke( books -> {
                                                out.println( author.getName() + " wrote " + books.size() + " books" );
                                                books.forEach( book -> out.println( book.getTitle() ) );
                                            } )
                                    )
                    )
                    .await().indefinitely();

            factory.withSession(
                            // retrieve the Author lazily from a Book
                            session -> session.find( Book.class, book1.getId() )
                                    // fetch a lazy field of the Book
                                    .chain( book -> fetch( book.getAuthor() )
                                            // print the lazy field
                                            .invoke( author -> out.printf( "%s wrote '%s'\n", author.getName(), book1.getTitle() ) )
                                    )
                    )
                    .await().indefinitely();

            factory.withSession(
                            // query the Book titles
                            session -> session.createQuery(
                                            "select title, author.name from Book order by title desc",
                                            Object[].class
                                    )
                                    .getResultList()
                                    .invoke( rows -> rows.forEach(
                                            row -> out.printf( "%s (%s)\n", row[0], row[1] )
                                    ) )
                    )
                    .await().indefinitely();

            factory.withSession(
                            // query the entire Book entities
                            session -> session.createQuery(
                                            "from Book book join fetch book.author order by book.title desc",
                                            Book.class
                                    )
                                    .getResultList()
                                    .invoke( books -> books.forEach(
                                            b -> out.printf(
                                                    "%s: %s (%s)\n",
                                                    b.getIsbn(),
                                                    b.getTitle(),
                                                    b.getAuthor().getName()
                                            )
                                    ) )
                    )
                    .await().indefinitely();

            factory.withSession(
                            // use a criteria query
                            session -> {
                                CriteriaQuery<Book> query = factory.getCriteriaBuilder().createQuery( Book.class );
                                Root<Author> a = query.from( Author.class );
                                Join<Author, Book> b = a.join( author1.getBooks() );
                                query.where( a.get( author1.getName() ).in( "Neal Stephenson", "William Gibson" ) );
                                query.select( b );
                                return session.createQuery( query ).getResultList().invoke(
                                        books -> books.forEach( book -> out.println( book.getTitle() ) )
                                );
                            }
                    )
                    .await().indefinitely();

            factory.withSession(
                            // retrieve a Book
                            session -> session.find( Book.class, book1.getId() )
                                    // fetch a lazy field of the Book
                                    .call( book -> session.fetch( book, book1.getPublished() )
                                            // print one lazy field
                                            .invoke( published -> out.printf(
                                                    "'%s' was published in %d\n",
                                                    book.getTitle(),
                                                    published.getYear()
                                            ) )
                                    )
                                    .call( book -> session.fetch( book, book1.coverImage )
                                            // print the other lazy field
                                            .invoke( coverImage -> out.println( new String( coverImage ) ) )
                                    )
                    )
                    .await().indefinitely();

            factory.withTransaction(
                            // retrieve a Book
                            (session, tx) -> session.find( Book.class, book2.getId() )
                                    // delete the Book
                                    .chain( session::remove )
                    )
                    .await().indefinitely();

            factory.withTransaction(
                            // delete all the Books in a transaction
                            (session, tx) -> session.createQuery( "delete Book" ).executeUpdate()
                                    //delete all the Authors
                                    .call( () -> session.createQuery( "delete Author" ).executeUpdate() )
                    )
                    .await().indefinitely();

        }
        finally {
            // remember to shut down the factory
            factory.close();
        }
    }}
    */

    @Entity
    @Table(name="authors")
    public static class Author {
        @Id
        @GeneratedValue
        private Integer id;

        @NotNull
        private String name;

        @OneToMany(mappedBy = "author", cascade = CascadeType.PERSIST)
        private List<Book> books = new ArrayList<>();

        public Author(String name) {
            super();
            this.name = name;
        }

        public Author() {}

        Integer getId() {
            return id;
        }

        String getName() {
            return name;
        }

        List<Book> getBooks() {
            return books;
        }
    }


    @Entity
    @Table(name="books")
    public static class Book {
        @Id
        @GeneratedValue
        private Integer id;

        private String isbn;

        @NotNull
        private String title;

        @Basic(fetch = LAZY)
        @NotNull
        private LocalDate published;

        @Basic(fetch = LAZY)
        public byte[] coverImage;

        @NotNull
        @ManyToOne(fetch = LAZY)
        private Author author;

        public Book(String isbn, String title, Author author, LocalDate published) {
            super();
            this.title = title;
            this.isbn = isbn;
            this.author = author;
            this.published = published;
            this.coverImage = ("Cover image for '" + title + "'").getBytes();
        }

        public Book() {}

        Integer getId() {
            return id;
        }

        String getIsbn() {
            return isbn;
        }

        String getTitle() {
            return title;
        }

        Author getAuthor() {
            return author;
        }

        LocalDate getPublished() {
            return published;
        }

        byte[] getCoverImage() {
            return coverImage;
        }
    }
}
