package org.hibernate.example.reactive;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

@Entity
@Table(name="books")
class Book {
	@Id @GeneratedValue Integer id;

	String isbn;

	String title;

	@ManyToOne
	Author author;

	Book(String isbn, String title, Author author) {
		this.title = title;
		this.isbn = isbn;
		this.author = author;
	}

	Book() {}
}
