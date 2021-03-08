package org.hibernate.reactive.example.session;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.ArrayList;
import java.util.List;

import static javax.persistence.CascadeType.PERSIST;

@Entity
@Table(name="authors")
class Author {
	@Id @GeneratedValue
	private Integer id;

	@NotNull @Size(max=100)
	private String name;

	@OneToMany(mappedBy = "author", cascade = PERSIST)
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
