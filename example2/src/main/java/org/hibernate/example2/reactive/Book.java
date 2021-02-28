package org.hibernate.example2.reactive;

import org.hibernate.annotations.SQLDelete;
import org.hibernate.annotations.SQLInsert;
import org.hibernate.annotations.SQLUpdate;
import org.hibernate.annotations.Subselect;

import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Past;
import javax.validation.constraints.Size;
import java.time.LocalDate;

import static javax.persistence.FetchType.LAZY;

@Entity
@Subselect("select author_id, isbn, published, title, id from books")
@SQLInsert(sql = "insert into books (author_id, isbn, published, title, id) values ($1, $2, $3, $4, $5)")
@SQLUpdate(sql = "")
@SQLDelete(sql = "delete from books where id=$1")
class Book {
	@Id @GeneratedValue
	private Integer id;

	@Size(min=13, max=13)
	private String isbn;

	@NotNull @Size(max=100)
	private String title;

	@Basic(fetch = LAZY)
	@NotNull @Past
	private LocalDate published;

	@NotNull
	@ManyToOne(fetch = LAZY)
	private Author author;

	Book(String isbn, String title, Author author, LocalDate published) {
		this.title = title;
		this.isbn = isbn;
		this.author = author;
		this.published = published;
	}

	Book() {}

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
}
