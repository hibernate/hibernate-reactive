/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.it.dirtychecking;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class Fruit {
	@Id
	private int id;

	// Dirty checking should not be confused by this initialization.
	private String name = "Banana";

	public int getId() {
		return id;
	}

	public Fruit setId(final int id) {
		this.id = id;
		return this;
	}

	public String getName() {
		return name;
	}

	public Fruit setName(final String name) {
		this.name = name;
		return this;
	}
}
