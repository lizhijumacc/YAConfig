package com.yaconfig.client.syncs;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.yaconfig.client.annotation.MySQLValue;
import com.yaconfig.client.injector.DataFrom;

@SyncType(from = DataFrom.MYSQL)
public class MySQLSync extends AbstractSync {

	@Override
	public void sync(String data,Field field,int from) {
		final MySQLValue mv = field.getAnnotation(MySQLValue.class);
		if(mv != null){
			String keyName = mv.keyName();
			String valueName = mv.valueName();
			String key = mv.key();
		    String username = mv.userName();
		    String password = mv.password();
			String tableName = mv.tableName();
			String connStr = mv.connStr();
			String sql = "update " + tableName + " set " + valueName + " = '" +
							data + "' where " + keyName + " = '" + key + "'";

		    Connection conn = null;
		    try {
		        conn = (Connection) DriverManager.getConnection(connStr, username, password);
		        PreparedStatement pstmt = (PreparedStatement)conn.prepareStatement(sql);
		        pstmt.execute();
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

}
