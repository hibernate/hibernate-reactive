/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.it.reference;

import java.util.Objects;

import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

@Entity(name = "Writer")
@Table(name = "TAuthor")
public class Author {

	@Id
	@GeneratedValue
	private Integer id;
	private String name;

	@ManyToOne(fetch = FetchType.LAZY, optional = false)
	private Book book;

	public Author() {
	}

	public Author(String name, Book book) {
		this.name = name;
		this.book = book;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Book getBook() {
		return book;
	}

	public void setBook(Book book) {
		this.book = book;
	}

	@Override
	public boolean equals(Object o) {
		if ( this == o ) {
			return true;
		}
		if ( o == null || getClass() != o.getClass() ) {
			return false;
		}
		Author author = (Author) o;
		return Objects.equals( name, author.name );
	}

	@Override
	public int hashCode() {
		return Objects.hash( name );
	}
}
