/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.example.session;

import java.time.LocalDate;

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
public class Book {
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

	public Book() {}

	public Book(String isbn, String title, Author author, LocalDate published) {
		this.title = title;
		this.isbn = isbn;
		this.author = author;
		this.published = published;
		this.coverImage = ("Cover image for '" + title + "'").getBytes();
	}

	public Integer getId() {
		return id;
	}

	public String getIsbn() {
		return isbn;
	}

	public String getTitle() {
		return title;
	}

	public Author getAuthor() {
		return author;
	}

	public LocalDate getPublished() {
		return published;
	}

	public byte[] getCoverImage() {
		return coverImage;
	}
}
