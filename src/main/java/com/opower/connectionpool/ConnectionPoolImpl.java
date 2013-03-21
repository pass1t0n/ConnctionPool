package com.opower.connectionpool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;



public class ConnectionPoolImpl implements Runnable, ConnectionPool {
  private String driver, url, username, password;
  private int maxConnections;
  private boolean waitIfBusy;
  private Vector<Connection> availableConnections, busyConnections;
  private boolean connectionPending = false;
  static Logger logger = Logger.getLogger(ConnectionPoolImpl.class);

  public ConnectionPoolImpl(String driver, String url, String username, String password,
                        	int initialConnections, int maxConnections, boolean waitIfBusy) throws SQLException{
	  logger.trace("ConnectionPoolImpl Ctor");
	  this.driver = driver;
	  this.url = url;
	  this.username = username;
	  this.password = password;
	  this.maxConnections = maxConnections;
	  this.waitIfBusy = waitIfBusy;
	  
	  if ( initialConnections<=0 || !isValidDBUrl(url)){
		  IllegalStateException e = new IllegalStateException("initialConnections<=0");
		  logger.error(e.getMessage(), e);
		  throw e;
	  }
    
	  if (initialConnections > maxConnections) {
		  logger.info("setting maxConnections to "+initialConnections);
		  initialConnections = maxConnections;
	  }
    
    availableConnections = new Vector<Connection>(initialConnections);
    busyConnections = new Vector<Connection>();
    for(int i=0; i<initialConnections; i++) {
      availableConnections.addElement(makeNewConnection());
    }
  }
  
  public void run() {
	  try {
      Connection connection = makeNewConnection();
      synchronized(this) {
        availableConnections.addElement(connection);
        connectionPending = false;
        logger.debug("run() new connection was created for any one who is waiting");
        notifyAll();
      }
    } catch(Exception e) { // SQLException or OutOfMemory
      // Give up on new connection and wait for existing one
      // to free up.
    }
  }
  
  public synchronized Connection getConnection() throws SQLException 
  {
	  if (!availableConnections.isEmpty()) {
      Connection existingConnection = (Connection)availableConnections.lastElement();
      int lastIndex = availableConnections.size() - 1;
      availableConnections.removeElementAt(lastIndex);
      // If connection on available list is closed (e.g.,
      // it timed out), then remove it from available list
      // and repeat the process of obtaining a connection.
      // Also wake up threads that were waiting for a
      // connection because maxConnection limit was reached.
      if (existingConnection.isClosed()) {
    	  logger.debug("getConnection() a closed connection has been removed.");
    	  notifyAll(); // Freed up a spot for anybody waiting
        return(getConnection());
      } else {
        busyConnections.addElement(existingConnection);
        return(existingConnection);
      }
    } else {
      
      // Three possible cases:
      // 1) You haven't reached maxConnections limit. So
      //    establish one in the background if there isn't
      //    already one pending, then wait for
      //    the next available connection (whether or not
      //    it was the newly established one).
      // 2) You reached maxConnections limit and waitIfBusy
      //    flag is false. Throw SQLException in such a case.
      // 3) You reached maxConnections limit and waitIfBusy
      //    flag is true. Then do the same thing as in second
      //    part of step 1: wait for next available connection.
      
      if ((totalConnections() < maxConnections) && !connectionPending) 
      {
        makeBackgroundConnection();
      } 
      else if (!waitIfBusy) {
    	  IllegalStateException e = new IllegalStateException("number of connection can not exceed maxConnections if waitIfBusy="+waitIfBusy);
    	  logger.error(e.getMessage(), e);
    	  throw e;
      }
      // Wait for either a new connection to be established
      // (if you called makeBackgroundConnection) or for
      // an existing connection to be freed up.
      try {
        wait();
      } 
      catch(InterruptedException ie) {}
      // Someone freed up a connection, so try again.
      return(getConnection());
    }
  }
  
  public synchronized void releaseConnection(Connection connection) {
    busyConnections.removeElement(connection);
    availableConnections.addElement(connection);
    // Wake up threads that are waiting for a connection
    notifyAll(); 
  }

  // You can't just make a new connection in the foreground
  // when none are available, since this can take several
  // seconds with a slow network connection. Instead,
  // start a thread that establishes a new connection,
  // then wait. You get woken up either when the new connection
  // is established or if someone finishes with an existing
  // connection.

  private void makeBackgroundConnection() {
    connectionPending = true;
    try {
      Thread connectThread = new Thread(this);
      connectThread.start();
    } catch(OutOfMemoryError oome) {
      // Give up on new connection
    }
  }

  

  // This explicitly makes a new connection. 
  private Connection makeNewConnection() throws SQLException 
  {
    try {
      // Load database driver if not already loaded
      Class.forName(driver);
      // Establish network connection to database
      Connection connection = DriverManager.getConnection(url, username, password);
      return(connection);
    } catch(ClassNotFoundException cnfe) {
    	SQLException e = new SQLException ("Can't find class for driver: " + driver);
    	logger.error(e.getMessage(), e);
    	throw e;
    }
  }

  public synchronized int totalConnections() {
    return(availableConnections.size() + busyConnections.size());
  }

  /** Close all the connections. Use with caution:
   *  be sure no connections are in use before
   *  calling. Note that you are not <I>required to
   *  call this when done with a ConnectionPool, since
   *  connections are guaranteed to be closed when
   *  garbage collected. But this method gives more control
   *  regarding when the connections are closed.
   */

  public synchronized void closeAllConnections() {
    closeConnections(availableConnections);
    availableConnections = new Vector<Connection>();
    closeConnections(busyConnections);
    busyConnections = new Vector<Connection>();
  }

  private void closeConnections(Vector<Connection> connections) {
    try {
      for(int i=0; i<connections.size(); i++) {
        Connection connection = connections.elementAt(i);
        if (!connection.isClosed()) {
          connection.close();
        }
      }
    } catch(SQLException sqle) {
      // Ignore errors; garbage collect anyhow
    }
  }
  
  public synchronized String toString() {
    String info =
      "ConnectionPool(" + url + "," + username + ")" +
      ", available=" + availableConnections.size() +
      ", busy=" + busyConnections.size() +
      ", max=" + maxConnections;
    return(info);
  }
  
  // validates url is a database url i.e.  jdbc:subprotocol:subname 
  private boolean isValidDBUrl(String url)
  {
	  Pattern p = Pattern.compile("jdbc:.+:.+");
	  Matcher m = p.matcher(url);
	  return m.matches();
  }
}