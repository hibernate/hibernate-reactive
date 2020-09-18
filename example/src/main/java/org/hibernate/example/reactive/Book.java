package org.hibernate.example.reactive;

import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Past;
import javax.validation.constraints.Size;

import java.time.LocalDate;

import static javax.persistence.FetchType.LAZY;

@Entity
@Table(name="books")
class Book {
	@Id @GeneratedValue
	Integer id;

	@Size(min=13, max=13)
	String isbn;

	public String getIsbn() {
		return isbn;
	}

	@NotNull @Size(max=100)
	String title;

	@Basic(fetch = LAZY)
	@NotNull @Past
	LocalDate published;

	@NotNull
	@ManyToOne(fetch = LAZY)
	Author author;

	Book(String isbn, String title, Author author, LocalDate published) {
		this.title = title;
		this.isbn = isbn;
		this.author = author;
		this.published = published;
	}

	Book() {}
}
