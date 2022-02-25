/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.example.nativesql;

import java.time.LocalDate;

import org.hibernate.annotations.SQLDelete;
import org.hibernate.annotations.SQLInsert;
import org.hibernate.annotations.SQLUpdate;
import org.hibernate.annotations.Subselect;

import jakarta.persistence.Basic;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Past;
import jakarta.validation.constraints.Size;

import static jakarta.persistence.FetchType.LAZY;

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
