/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.it.quarkus.qe.database;

import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.NamedQuery;
import jakarta.persistence.Table;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;


@Entity
@Table(name = "books")
@NamedQuery(name = "find_by_title_prefix", query = "from Book where title like :prefix")
public class Book {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Integer id;

	@NotNull
	@Size(max = 10)
	private String title;

	@NotNull
	@ManyToOne
	private Author author;

	@Convert(converter = ISBNConverter.class)
	private long isbn;

	public Book() {
	}

	public Book(String title) {
		this.title = title;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public @NotNull @Size(max = 10) String getTitle() {
		return title;
	}

	public void setTitle(@NotNull @Size(max = 10) String title) {
		this.title = title;
	}

	public @NotNull Author getAuthor() {
		return author;
	}

	public void setAuthor(@NotNull Author author) {
		this.author = author;
	}

	public long getIsbn() {
		return isbn;
	}

	public void setIsbn(long isbn) {
		this.isbn = isbn;
	}

	@Override
	public String toString() {
		return id + ":" + title + ":" + isbn + ":" + author;
	}
}
