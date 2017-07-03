package com.yaconfig.client.fetcher;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.yaconfig.client.annotation.MySQLValue;
import com.yaconfig.client.injector.DataFrom;

public class MySQLFetcher extends AbstractFetcher{
	public MySQLFetcher(Field field) {
		super(field);
	}

	@Override
	public void fetch(FetchCallback callback) {
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
