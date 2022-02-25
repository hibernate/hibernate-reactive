/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.example.nativesql;

import org.hibernate.annotations.SQLDelete;
import org.hibernate.annotations.SQLInsert;
import org.hibernate.annotations.SQLUpdate;
import org.hibernate.annotations.Subselect;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.ArrayList;
import java.util.List;

@Entity
@Subselect("select name, id from authors")
@SQLInsert(sql = "insert into authors (name, id) values ($1, $2)")
@SQLUpdate(sql = "")
@SQLDelete(sql = "delete from authors where id = $1")
class Author {
	@Id @GeneratedValue
	private Integer id;

	@NotNull @Size(max=100)
	private String name;

	@OneToMany(mappedBy = "author")
	private List<Book> books = new ArrayList<>();

	Author(String name) {
		this.name = name;
	}

	Author() {}

	Integer getId() {
		return id;
	}

	String getName() {
		return name;
	}

	List<Book> getBooks() {
		return books;
	}
}
