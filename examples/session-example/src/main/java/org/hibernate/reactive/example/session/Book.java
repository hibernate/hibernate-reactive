/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.example.session;

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
	private Integer id;

	@Size(min=13, max=13)
	private String isbn;

	@NotNull @Size(max=100)
	private String title;

	@Basic(fetch = LAZY)
	@NotNull @Past
	private LocalDate published;

	@Basic(fetch = LAZY)
	public byte[] coverImage;

	@NotNull
	@ManyToOne(fetch = LAZY)
	private Author author;

	Book(String isbn, String title, Author author, LocalDate published) {
		this.title = title;
		this.isbn = isbn;
		this.author = author;
		this.published = published;
		this.coverImage = ("Cover image for '" + title + "'").getBytes();
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

	byte[] getCoverImage() {
		return coverImage;
	}
}
