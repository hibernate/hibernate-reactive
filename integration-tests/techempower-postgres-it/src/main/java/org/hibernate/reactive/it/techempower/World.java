/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.it.techempower;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class World {

	@Id
	private int id;
	private int randomNumber;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getRandomNumber() {
		return randomNumber;
	}

	public void setRandomNumber(int randomNumber) {
		this.randomNumber = randomNumber;
	}

	@Override
	public String toString() {
		return id + ":" + randomNumber;
	}
}
