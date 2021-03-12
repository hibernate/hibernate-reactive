package org.hibernate.reactive.example.nativesql;

import javax.annotation.Generated;
import javax.persistence.metamodel.ListAttribute;
import javax.persistence.metamodel.SingularAttribute;
import javax.persistence.metamodel.StaticMetamodel;

@Generated(value = "org.hibernate.jpamodelgen.JPAMetaModelEntityProcessor")
@StaticMetamodel(Author.class)
public abstract class Author_ {

	public static volatile ListAttribute<Author, Book> books;
	public static volatile SingularAttribute<Author, String> name;
	public static volatile SingularAttribute<Author, Integer> id;

	public static final String BOOKS = "books";
	public static final String NAME = "name";
	public static final String ID = "id";

}

