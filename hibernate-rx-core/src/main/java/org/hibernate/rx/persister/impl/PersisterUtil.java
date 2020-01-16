package org.hibernate.rx.persister.impl;

public class PersisterUtil {

    public static String[] lower(String[] strings) {
        for (int i = 0; i < strings.length; i++) {
            strings[i] = strings[i].toLowerCase();
        }
        return strings;
    }

    public static String lower(String string) {
        return string==null ? null : string.toLowerCase();
    }

    public static String fixSqlParameters(String sql) {
        int num = 1;
        while ( sql.contains("?") ) {
            sql = sql.replaceFirst("\\?", "\\$" + num );
            num++;
        }
        return sql;
    }
}
