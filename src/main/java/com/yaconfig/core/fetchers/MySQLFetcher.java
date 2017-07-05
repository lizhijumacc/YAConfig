package com.yaconfig.core.fetchers;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.yaconfig.core.DataFrom;
import com.yaconfig.core.annotation.MySQLValue;

@FetcherType(from = DataFrom.MYSQL)
public class MySQLFetcher extends AbstractFetcher{
	
	public MySQLFetcher() {
		
	}

	@Override
	public void fetch(Field field,FetchCallback callback) {
		final MySQLValue mv = field.getAnnotation(MySQLValue.class);
		String keyName = mv.keyName();
		String valueName = mv.valueName();
		String key = mv.key();
	    String username = mv.userName();
	    String password = mv.password();
		String tableName = mv.tableName();
		String connStr = mv.connStr();
		String sql = "select " + valueName + " from " + tableName + " where " +
						keyName + " = '" + key + "'";

	    Connection conn = null;
	    try {
	        conn = (Connection) DriverManager.getConnection(connStr, username, password);
	        PreparedStatement pstmt = (PreparedStatement)conn.prepareStatement(sql);
	        ResultSet rs = pstmt.executeQuery();
	        rs.next();
	        callback.dataFetched(rs.getString(1), field, DataFrom.MYSQL);
	    } catch (SQLException e) {
	        e.printStackTrace();
	    }finally{
	    	try {
				if(conn!=null)conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
	    }
	    
	}
}
