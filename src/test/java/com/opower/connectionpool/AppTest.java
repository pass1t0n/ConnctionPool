package com.opower.connectionpool;

import java.net.MalformedURLException;
import org.apache.log4j.PropertyConfigurator;
import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.junit.Before;

/**
 * Unit test for Connection pool.
 */
public class AppTest extends TestCase
{
	private String driver, url, username, password;
	private int initialConnections, maxConnections;
	private boolean waitIfBusy;
	private final String LOG4J_CFG_FILE = "log4j.cfg";
	
	/**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }
    
    @Before 
    public void setUp() { 
    	driver = "com.mysql.jdbc.Driver";
    	url = "jdbc:mysql://localhost/hibernatetutorial";
    	username = "root";
    	password = "";
    	initialConnections = 2;
    	maxConnections = 10;
    	waitIfBusy = true; 
    	PropertyConfigurator.configure(LOG4J_CFG_FILE);
    }


    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /***** Making sure that exceptions are being propagated and logged properly *****/
    public void test_wrongDriverName() throws MalformedURLException
    {
        try{
        	new ConnectionPoolImpl("", url, username, password, initialConnections, maxConnections, waitIfBusy);
        }
        catch (SQLException e){
        	assertTrue(true);
        }
    }
    
    public void test_malformedDriverURL() throws SQLException
    {
        try{
        	new ConnectionPoolImpl(driver, "sdf:sdfe", username, password, initialConnections, maxConnections, waitIfBusy);
        }
        catch (IllegalStateException e){
        	assertTrue(true);
        }
    }
    
    public void test_wrongUserName() throws SQLException
    {
    	try {
    		new ConnectionPoolImpl(driver, url, "WRONG_USER_NAME", password, initialConnections, maxConnections, waitIfBusy);
    	}
        catch (SQLException e){
        	assertTrue(true);
        }
    }
    
    public void test_wrongPassword() throws SQLException
    {
    	try {
    		new ConnectionPoolImpl(driver, url, username, "a", initialConnections, maxConnections, waitIfBusy);
    	}
        catch (SQLException e){
        	assertTrue(true);
        }
    }
    
    public void test_initialNumberOfConnections() throws SQLException
    {
    	try {
    		new ConnectionPoolImpl(driver, url, username, password, -1, maxConnections, waitIfBusy);
    	}
        catch (IllegalStateException e){
        	assertTrue(true);
        }
    }
    
    /***** End of exceptions test *****/
    
    // if waitIsBusy=false then exceeding the number of connections should throw an IllegalStateException
    public void test_outOfConnections()
    {
    	int numOfConnection = 3; 
    	try {
    		ConnectionPool cp = new ConnectionPoolImpl(driver, url, username, password, numOfConnection, numOfConnection, false);
    		
    		for (int i=0 ; i<numOfConnection+1 ; i++)
    			cp.getConnection();
    	}
        catch (IllegalStateException e){
        	assertTrue(true);
        } catch (Exception e) {
        	assertTrue(false);
		}
    }
    
 	// if waitIsBusy=false and we have not exceeded the # of connection an exception should not be thrown
    public void test_createConnection()
    {
    	int numOfConnection = 3; 
    	try {
    		ConnectionPool cp = new ConnectionPoolImpl(driver, url, username, password, numOfConnection, numOfConnection+1, false);
    		
    		for (int i=0 ; i<numOfConnection+1 ; i++)
    			cp.getConnection();
    	}
        catch (Exception e){
        	assertTrue(false);
        }
    }
    
    // in case connection pool waitIfBusy=true the previous test should pass 
    public void test_checkConnections()
    {
    	try {
    		ConnectionPool cp = new ConnectionPoolImpl(driver, url, username, password, initialConnections, initialConnections, true);
    		
    		//the last connection will wait
    		for (int i=0 ; i<initialConnections+1 ; i++)
    			(new ConnectionPoolTester(cp)).run();
    		
    		
    		assertTrue(((ConnectionPoolImpl)cp).totalConnections()==initialConnections);
    	}
        catch (Exception e){
        	assertTrue(false);
        }
    }
    
}
