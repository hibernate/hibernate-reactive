package org.hibernate.reactive.example.nativesql;

import org.hibernate.annotations.SQLDelete;
import org.hibernate.annotations.SQLInsert;
import org.hibernate.annotations.SQLUpdate;
import org.hibernate.annotations.Subselect;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
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
