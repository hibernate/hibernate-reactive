package org.hibernate.example.reactive;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import static javax.persistence.FetchType.LAZY;

@Entity
@Table(name="books")
class Book {
	@Id @GeneratedValue
	Integer id;

	@NotNull @Size(min=13, max=13)
	String isbn;

	@NotNull @Size(max=100)
	String title;

	@NotNull @ManyToOne(fetch = LAZY)
	Author author;

	Book(String isbn, String title, Author author) {
		this.title = title;
		this.isbn = isbn;
		this.author = author;
	}

	Book() {}
}
