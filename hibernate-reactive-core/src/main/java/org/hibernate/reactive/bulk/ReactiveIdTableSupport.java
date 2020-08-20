/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.bulk;

import org.hibernate.dialect.DB297Dialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MySQL8Dialect;
import org.hibernate.dialect.PostgreSQL10Dialect;
import org.hibernate.hql.spi.id.IdTableSupportStandardImpl;

/**
 * Hardcoded support for temporary id tables for the three supported
 * databases. (Since the {@link Dialect} does not expose this stuff
 * in a useful form.)
 *
 * @author Gavin King
 */
class ReactiveIdTableSupport extends IdTableSupportStandardImpl {
    private final Dialect dialect;

    public ReactiveIdTableSupport(Dialect dialect) {
        this.dialect = dialect;
    }

    @Override
    public String generateIdTableName(String baseName) {
        return "ht_" + baseName;
    }

    @Override
    public String getCreateIdTableCommand() {
        if (dialect instanceof PostgreSQL10Dialect) {
            return "create temporary table";
        }
        else if (dialect instanceof MySQL8Dialect) {
            return "create temporary table if not exists";
        }
        else if (dialect instanceof DB297Dialect) {
            //TODO: use 'create global temporary table ... not logged'
//				return "create global temporary table";
            return "create table";
        } else {
            return "create local temporary table";
        }
    }

    @Override
    public String getDropIdTableCommand() {
        if (dialect instanceof PostgreSQL10Dialect) {
            return "drop table";
        }
        else if (dialect instanceof MySQL8Dialect) {
            return "drop temporary table";
        }
        else {
            return "drop table";
        }
    }

    @Override
    public String getCreateIdTableStatementOptions() {
        if (dialect instanceof DB297Dialect) {
            //TODO: use 'create global temporary table ... not logged'
//				return "not logged";
        }
        return null;
    }
}
