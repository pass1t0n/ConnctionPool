package com.opower.connectionpool;

import static org.easymock.EasyMock.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

class ConnectionPoolTester implements Runnable {
	
	ConnectionPool cp;
	Connection conn;
	Statement mock = createMock(Statement.class);;
	String sqlQuery = "INSERT || UPDATE || DELETE SQL statement to the db";
	ConnectionPoolTester(ConnectionPool cp) throws SQLException{
		this.cp = cp;
		expect(mock.executeUpdate(sqlQuery)).andReturn(0);
		replay(mock);
	}

	public void run(){
		try {
			conn = cp.getConnection();
			Statement st = conn.createStatement();
			mock.executeUpdate(sqlQuery);
			cp.releaseConnection(conn);
			
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
	}
}
