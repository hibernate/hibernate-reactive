/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.it.quarkus.qe.database;

import static jakarta.persistence.CascadeType.PERSIST;

import java.util.ArrayList;
import java.util.List;


import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;


@Entity
@Table(name = "authors")
public class Author {

	@Id
	@GeneratedValue
	private Long id;

	@NotNull
	@Size(max = 10)
	private String name;

	@OneToMany(cascade = PERSIST)
	@JoinColumn(name = "author")
	private List<Book> books = new ArrayList<>();

	public Author(String name) {
		this.name = name;
	}

	public Author() {
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public @NotNull @Size(max = 10) String getName() {
		return name;
	}

	public void setName(@NotNull @Size(max = 10) String name) {
		this.name = name;
	}

	public List<Book> getBooks() {
		return books;
	}

	public void setBooks(List<Book> books) {
		this.books = books;
	}

	@Override
	public String toString() {
		return id + ":" + name;
	}
}
