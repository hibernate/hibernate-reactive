package org.hibernate.id;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface RxBinder {
	void bindValues(PreparedStatement ps) throws SQLException;
	Object getEntity();
}
