/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.it;

import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.Id;

import static javax.persistence.FetchType.LAZY;

@Entity
public class Crew {
	@Id
	private Long id;

	private String name;

	@Basic(fetch = LAZY)
	private String role;

	@Basic(fetch = LAZY)
	public String fate;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getRole() {
		return role;
	}

	public void setRole(String role) {
		this.role = role;
	}

	public String getFate() {
		return fate;
	}

	public void setFate(String fate) {
		this.fate = fate;
	}

	@Override
	public String toString() {
		return id + ":" + name + ":" + role + ":" + fate;
	}
}
