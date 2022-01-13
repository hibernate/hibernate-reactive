/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.it.verticle;

import java.math.BigDecimal;
import java.util.Objects;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class Product {

	@Id
	private Long id;

	@Column(unique = true)
	private String name;

	@Column(nullable = false)
	private BigDecimal price;

	public Product() {
	}

	public Product(int index) {
		this.id = Long.valueOf( index );
		this.name = "Product " + index;
		this.price = new BigDecimal( index + ".00" );
	}

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

	public BigDecimal getPrice() {
		return price;
	}

	public void setPrice(BigDecimal price) {
		this.price = price;
	}

	@Override
	public boolean equals(Object o) {
		if ( this == o ) {
			return true;
		}
		if ( !( o instanceof Product ) ) {
			return false;
		}
		Product product = (Product) o;
		return Objects.equals( name, product.name )
				&& Objects.equals( price, product.price );
	}

	@Override
	public int hashCode() {
		return Objects.hash( name, price );
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder( "Product{" );
		sb.append( id );
		sb.append( ", " ).append( name );
		sb.append( ", " ).append( price );
		sb.append( '}' );
		return sb.toString();
	}
}
