package org.hibernate.reactive.example.nativesql;

import java.time.LocalDate;
import javax.annotation.Generated;
import javax.persistence.metamodel.SingularAttribute;
import javax.persistence.metamodel.StaticMetamodel;

@Generated(value = "org.hibernate.jpamodelgen.JPAMetaModelEntityProcessor")
@StaticMetamodel(Book.class)
public abstract class Book_ {

	public static volatile SingularAttribute<Book, Author> author;
	public static volatile SingularAttribute<Book, String> isbn;
	public static volatile SingularAttribute<Book, Integer> id;
	public static volatile SingularAttribute<Book, LocalDate> published;
	public static volatile SingularAttribute<Book, String> title;

	public static final String AUTHOR = "author";
	public static final String ISBN = "isbn";
	public static final String ID = "id";
	public static final String PUBLISHED = "published";
	public static final String TITLE = "title";

}

