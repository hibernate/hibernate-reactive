/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.schema.validate;

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
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Version;

import org.hibernate.annotations.Type;

@Entity(name = "BasicTypesTestEntity")
public class BasicTypesTestEntity {

	@Id
	@GeneratedValue
	Integer id;
	@Version
	Integer version;
	String aString;

	boolean primativeBoolean;
	int primativeInt;
	long primativeLong;
	float primativeFloat;
	double primativeDouble;
	byte primativeByte;
	byte[] primativeBytes;

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
	Date date;
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
	@Temporal(TemporalType.TIME)
	Date dateAsTime;

	@javax.persistence.Basic
	Serializable thing;

	UUID uuid;

	BigDecimal bigDecimal;
	BigInteger bigInteger;

	Instant instant;

	Duration duration;

	Serializable serializable;

	public BasicTypesTestEntity() {
	}

	public String getEntityName() {
		return "BasicTypesTestEntity";
	}

}
