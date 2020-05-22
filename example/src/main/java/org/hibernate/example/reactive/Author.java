package org.hibernate.example.reactive;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.util.ArrayList;
import java.util.List;

import static javax.persistence.CascadeType.PERSIST;

@Entity
@Table(name="authors")
class Author {
	@Id
	@GeneratedValue
	Integer id;

	String name;

	@OneToMany(mappedBy = "author", cascade = PERSIST)
	List<Book> books = new ArrayList<>();

	Author(String name) {
		this.name = name;
	}

	Author() {}
}
