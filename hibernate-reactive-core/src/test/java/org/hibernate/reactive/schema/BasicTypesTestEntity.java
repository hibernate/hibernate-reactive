/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.schema;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;
import jakarta.persistence.Version;

import org.hibernate.annotations.Type;

@Entity(name = "BasicTypesTestEntity")
@Table(name = BasicTypesTestEntity.TABLE_NAME)
public class BasicTypesTestEntity {

	public static final String TABLE_NAME = "BASIC_TYPES_TABLE";

	String name;

	@Id
	@GeneratedValue
	Integer id;
	@Version
	Integer version;
	String aString;

	boolean primitiveBoolean;
	int primitiveInt;
	long primitiveLong;
	float primitiveFloat;
	double primitiveDouble;
	byte primitiveByte;
	byte[] primitiveBytes;

	Boolean fieldBoolean;
	Integer fieldInteger;
	Long fieldLong;
	Float fieldFloat;
	Double fieldDouble;
	Byte fieldByte;

	@Type(type = "true_false")
	Boolean booleanTrueFalse;

	@Type(type = "yes_no")
	Boolean booleanYesNo;

	@Type(type = "numeric_boolean")
	Boolean booleanNumeric;

	URL url;

	TimeZone timeZone;

	@Temporal(TemporalType.DATE)
	Date someDate;
	@Temporal(TemporalType.TIME)
	Date dateAsTime;
	@Temporal(TemporalType.TIMESTAMP)
	Date dateAsTimestamp;

	@Temporal(TemporalType.DATE)
	Calendar calendarAsDate;
	@Temporal(TemporalType.TIMESTAMP)
	Calendar calendarAsTimestamp;

	@Column(name = "localdayte")
	LocalDate localDate;
	@Column(name = "alocalDT")
	LocalDateTime localDateTime;
	LocalTime theLocalTime;

	@jakarta.persistence.Basic
	Serializable thing;

	UUID uuid;

	BigDecimal bigDecimal;
	BigInteger bigInteger;

	Instant instant;

	Duration duration;

	Serializable serializable;

	public BasicTypesTestEntity() {
	}
	public BasicTypesTestEntity(String name) {
		this.name = name;
	}
}
