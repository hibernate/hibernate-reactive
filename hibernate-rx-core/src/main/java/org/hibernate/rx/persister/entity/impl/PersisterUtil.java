package org.hibernate.rx.persister.entity.impl;

public class PersisterUtil {
	public static String fixSqlParameters(String sql) {
		if (sql == null) {
			return null;
		}
		int num = 1;
		while ( sql.contains("?") ) {
			sql = sql.replaceFirst("\\?", "\\$" + num );
			num++;
		}
		return sql;
	}
}
