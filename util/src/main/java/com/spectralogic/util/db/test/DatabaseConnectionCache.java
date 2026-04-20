/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.test;


import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;


/**
 * Intentionally package private.
 */
final class DatabaseConnectionCache
{
    
    /**
     * Intentionally package private.
     */
    static Connection establishDbConnection( final String username,
                             final String password, final String connectionUrl )
    {
        final String qn = new StringBuilder( 200 ).append( connectionUrl )
                                                  .append( DB_CONN_URL_DELIM )
                                                  .append( username )
                                                  .append( DB_CONN_URL_DELIM )
                                                  .append( password ).toString();
        BlockingQueue< Connection > que = null;
        synchronized( DB_CONN_QUEUES_MAP )
        {
            que = DB_CONN_QUEUES_MAP.get( qn );
            if ( null == que )
            {
                QUEUE_NAME_TO_CONN_COUNT.put( qn, new AtomicInteger( 0 ) );
                que = new LinkedBlockingQueue<>();
                DB_CONN_QUEUES_MAP.put( qn, que );
            }
        }

        try
        {   
            if ( MIN_DB_CONN_QUEUE_SIZE > que.size() )
            {
                MIGHT_NEED_MORE_CONNS_QUEUE.put( qn );
                synchronized ( DB_CONN_CACHE_NAME )
                {
                    DB_CONN_CACHE_NAME.notifyAll();
                }
            }
            if ( LOG.isDebugEnabled() )
            {
                LOG.debug( new StringBuilder( 200 ).append( que.size() )
                                  .append( " DB conns in que " ).append( qn ) );
            }
            return que.take();
        }
        catch ( final InterruptedException ex )
        {
            LOG.error( "Interrupted." );
            Thread.currentThread().interrupt();
            throw new RuntimeException( ex );
        }
    }
    
    
    /**
     * Intentionally package private.
     */
    static void closeConnection( final Object p )
    {
        try
        {
            final DbConnIH ih = (DbConnIH)Proxy.getInvocationHandler( p );
            final Connection c = ih.m_dbConn;
            final String qn = CONN_TO_QUEUE_NAME.get( c );
            if ( null == qn )
            {
                throw new IllegalStateException(
                "DB conns not created by this cache should not be closed by it." );
            }
            
            TIME_SINCE_QUE_LAST_USED_MAP.get( qn ).reset();
            
            if ( c.isClosed() )
            {
                CONN_TO_QUEUE_NAME.remove( c );
                QUEUE_NAME_TO_CONN_COUNT.get( qn ).decrementAndGet();
                LOG.error( "\n\n\nCached DB conn to " + qn +
                           " passed back to cache already in closed state." +
                           " Not a good thing.\n\n\n" );
                return;
            }
            
            if ( ! c.getAutoCommit() )
            {
                c.commit();
                c.setAutoCommit( true );
            }
            c.clearWarnings();
        
            final Queue< Connection > q = DB_CONN_QUEUES_MAP.get( qn );
            
            if ( null == q || 
                 MAX_DB_CONN_QUEUE_SIZE < q.size() ||
                 ! q.offer( (Connection)p ) )
            {
                CONN_TO_QUEUE_NAME.remove( c );
                c.close();
                QUEUE_NAME_TO_CONN_COUNT.get( qn ).decrementAndGet();
                LOG.error( "\n\n\nClosed a cached DB conn to " + qn +
                           "\nThe DB conn cache's min and max queues size" +
                           " params likely need adjusted to avoid this.\n\n\n" );
            }
            if ( LOG.isDebugEnabled() && null != q )
            {
                LOG.debug( new StringBuilder( 200 ).append( q.size() )
                                  .append( " DB conns in que " ).append( qn ) );
            }
        }
        catch ( final SQLException ex )
        {
            LOG.error( ex );
        }
    }

    
    private static final BlockingQueue<String> MIGHT_NEED_MORE_CONNS_QUEUE =
                                                    new LinkedBlockingQueue<>();
    
    private static final Map< String, BlockingQueue< Connection > >
                             DB_CONN_QUEUES_MAP = new ConcurrentSkipListMap<>(); 
                             
    private static final Map< Connection, String > CONN_TO_QUEUE_NAME =
             Collections.synchronizedMap( new HashMap< Connection, String >() );
    
    private static final Map< String, AtomicInteger > QUEUE_NAME_TO_CONN_COUNT =
                                                  new ConcurrentSkipListMap<>(); 
    
    private static final String DB_CONN_URL_DELIM = "---";
    
    // Note: One test class, JobServiceImpl_Test, of the many hundreds in the
    // Frontend test bed, uses a connection pool in such a way that it fails
    // if there are less than 16 connections in the DB conn queue it uses (so
    // we set the max size to 18 for good measure). All other 3600+ test methods
    // get by with a max conn queue size of 10.
    private static final int MAX_DB_CONN_QUEUE_SIZE = 18;
    private static final int MIN_DB_CONN_QUEUE_SIZE = 5;
    
    private static final Class< ? >[] CONNECTION_CLASS_ARRAY =
                 { Connection.class, org.postgresql.core.BaseConnection.class };
    
    
    private static final class DbConnIH implements InvocationHandler
    {
        private DbConnIH()
        {
            m_dbConn = null;
        }
        
        private DbConnIH( final Connection c )
        {
            Validations.verifyNotNull( "c", c );
            m_dbConn = c;
        }
        
        @Override
        public Object invoke( final Object c,
                              final Method m,
                              final Object[] a ) throws Throwable
        {
            if( "close".equals( m.getName() ) )
            {
                closeConnection( c );
                return null;
            }
            return m.invoke( m_dbConn, a );
        }
        
        private final Connection m_dbConn;
    }
    
    
    private static final class DBConnCacher extends Thread
    {
        private DBConnCacher()
        {
            super();
        }
        
        @Override
        public void run()
        {
            BlockingQueue<Connection> que = null;
            String dbInfoStr = null;
            String[] dbInfoArry = null;
            Connection conn = null;
            Duration d = null;
            
            while ( true )
            {
                dbInfoStr = MIGHT_NEED_MORE_CONNS_QUEUE.poll();
                while ( null != dbInfoStr )
                {
                    dbInfoArry = dbInfoStr.split( DB_CONN_URL_DELIM );
                    que = DB_CONN_QUEUES_MAP.get( dbInfoStr );
                        
                    if ( MAX_DB_CONN_QUEUE_SIZE >
                               QUEUE_NAME_TO_CONN_COUNT.get( dbInfoStr ).get() )
                    {
                        try
                        {   
                            // Ensure Postgres driver is loaded for DriverManager:
                            Class.forName( "org.postgresql.Driver" );
                            DriverManager.setLoginTimeout( 10 );
                            
                            conn = DriverManager.getConnection(
                                  dbInfoArry[0], dbInfoArry[1], dbInfoArry[2] );
                            CONN_TO_QUEUE_NAME.put( conn, dbInfoStr );
                            
                            que.put( (Connection)Proxy.newProxyInstance(
                               DatabaseConnectionCache.class.getClassLoader(),
                               CONNECTION_CLASS_ARRAY, new DbConnIH( conn ) ) );
                            
                            QUEUE_NAME_TO_CONN_COUNT.get( dbInfoStr )
                                                             .incrementAndGet();
                            
                            d = TIME_SINCE_QUE_LAST_USED_MAP.get( dbInfoStr );
                            if ( null == d )
                            {
                                TIME_SINCE_QUE_LAST_USED_MAP.put(
                                                    dbInfoStr, new Duration() );
                            }
                            else
                            {
                                d.reset();
                            }
                        }
                        catch ( final InterruptedException ex )
                        {
                            LOG.error( "Interrupted", ex );
                            Thread.currentThread().interrupt();
                            throw new RuntimeException( ex );
                        }
                        catch ( final SQLException ex )
                        {
                            LOG.error( "Failed to get a DB connection.", ex );
                            throw new RuntimeException( ex );
                        }
                        catch ( final ClassNotFoundException ex )
                        {
                            LOG.error(
                            "Could not load class Postgres driver manager.", ex );
                            throw new RuntimeException( ex );
                        }
                    } // if
                    
                    dbInfoStr = MIGHT_NEED_MORE_CONNS_QUEUE.poll();
                    
                } // inner while
                
                synchronized ( DB_CONN_CACHE_NAME )
                {
                    try
                    {
                        /* There's a window during which the notifyAll() signal
                         * of a thread that might need a connection will be
                         * missed by this thread. Therefore, never sleep more
                         * than 5s before checking the "cache some conns queue".
                         */
                        DB_CONN_CACHE_NAME.wait( 5000 );
                    }
                    catch ( final InterruptedException ex )
                    {
                        LOG.error( "Interrupted", ex );
                        Thread.currentThread().interrupt();
                        throw new RuntimeException( ex );
                    }
                }
            } // while ( true )
        }
    }
    
    
    private final static String DB_CONN_CACHE_NAME = "DB_CONN_CACHE";
    
    static
    {
        final DBConnCacher dbConnCacher1 = new DBConnCacher();
        dbConnCacher1.setName( DB_CONN_CACHE_NAME );
        dbConnCacher1.setDaemon( true );
        dbConnCacher1.start();
    }
    
    
    private static final Map< String, Duration >
                 TIME_SINCE_QUE_LAST_USED_MAP = new ConcurrentSkipListMap<>(); 

                 
    /**
     *  Closes all connections in a DB conn que if none of them have been used
     *  in the last N seconds. 
     */
    private static final class DBConnCacheCleaner extends Thread
    {
        private DBConnCacheCleaner()
        {
            super();
        }
                        
        @Override
        public void run()
        {
            final int maxSinceLastUsedSeconds = 30;
            
            while ( true )
            {
               try
               {
                   Thread.sleep( 15000 );
               }
               catch ( final InterruptedException ex )
               {
                   LOG.info( "Interrupted", ex );
                   Thread.currentThread().interrupt();
                   throw new RuntimeException( ex );
               }
                   
               Queue< Connection > que = null;
               String queName = null;
               Connection c = null;
               DbConnIH ih = null;
                   
               for( Map.Entry< String, Duration > e :
                                       TIME_SINCE_QUE_LAST_USED_MAP.entrySet() )
               {
                   queName = e.getKey();
                   que = DB_CONN_QUEUES_MAP.get( queName );
                           
                   // Always check the current duration since the queue was last
                   // used before closing the next conn:
                   while ( maxSinceLastUsedSeconds <
                                   TIME_SINCE_QUE_LAST_USED_MAP
                                           .get( queName ).getElapsedSeconds() )
                   {
                       c = que.poll();
                       if ( null == c )
                       {
                           break;
                       }
                       QUEUE_NAME_TO_CONN_COUNT.get( queName ).decrementAndGet();
                           
                       LOG.info( "Closing a conn to a DB that's not been " +
                                 "accessed in at least " +
                                 maxSinceLastUsedSeconds + "s : " + queName );
                       
                       // Close the acutal Connection, not the proxied one,
                       // because the proxied one does not actually close:
                       ih = (DbConnIH)Proxy.getInvocationHandler( c );
                       c = ih.m_dbConn;
                       
                       CONN_TO_QUEUE_NAME.remove( c );
                       try
                       {
                           c.close();
                       }
                       catch ( final SQLException ex )
                       {
                           LOG.error( ex );
                       }
                   }
               } // for
            } // while ( true )
        } // run()
    }
                 
    
    static
    {
        final DBConnCacheCleaner dbConnCacheCleaner = new DBConnCacheCleaner();
        dbConnCacheCleaner.setName( "DB_CONN_CACHE_CLEANER" );
        dbConnCacheCleaner.setDaemon( true );
        dbConnCacheCleaner.start();
    } 
    
    
    private static final Logger LOG =
                              Logger.getLogger( DatabaseConnectionCache.class );
    /*
    static
    {
        LOG.setLevel( Level.DEBUG );
    }
    */
}
