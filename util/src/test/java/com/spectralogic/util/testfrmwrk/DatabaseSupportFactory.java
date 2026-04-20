/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.testfrmwrk;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.spectralogic.util.cache.CacheResultProvider;
import com.spectralogic.util.db.codegen.SqlCodeGenerator;
import com.spectralogic.util.db.lang.DatabaseUtils;
import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.db.manager.DataSource;
import com.spectralogic.util.db.manager.frmwrk.ConnectionPool;
import com.spectralogic.util.db.manager.frmwrk.ConnectionPoolFactory;
import com.spectralogic.util.db.manager.postgres.PostgresDataManager;
import com.spectralogic.util.db.manager.postgres.PostgresDataSource;
import com.spectralogic.util.db.service.BeansServiceManagerImpl;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.db.test.TestConnectionPool;
import com.spectralogic.util.db.test.TestDataSource;
import com.spectralogic.util.db.validate.DataSourceValidator;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.notification.dispatch.NotificationEventDispatcher;
import com.spectralogic.util.notification.dispatch.TransactionNotificationEventDispatcher;
import com.spectralogic.util.thread.wp.SystemWorkPool;

public final class DatabaseSupportFactory
{
    public static DatabaseSupport getSupport(
            final Class< ? > seedClassInPackageToSearchForDataTypes, 
            final Class< ? > seedClassInPackageToSearchForServices )
    {
        return getSupport( true,
                           seedClassInPackageToSearchForDataTypes,
                           seedClassInPackageToSearchForServices );
    }
    
    
    public static DatabaseSupport getSupport(
            final boolean useTestDataSource, 
            final Class< ? > seedClassInPackageToSearchForDataTypes, 
            final Class< ? > seedClassInPackageToSearchForServices )
    {
        return getSupport( useTestDataSource,
                toSet( seedClassInPackageToSearchForDataTypes ), 
                toSet( seedClassInPackageToSearchForServices ) );
    }
    
    
    private static Set< Class< ? > > toSet( final Class< ? > clazz )
    {
        final Set< Class< ? > > retval = new HashSet<>();
        retval.add( clazz );
        return retval;
    }
    
    
    public static DatabaseSupport getSupport(
            final Set< Class< ? > > seedClassesInPackagesToSearchForDataTypes, 
            final Set< Class< ? > > seedClassesInPackagesToSearchForServices )
    {
        return getSupport( true,
                seedClassesInPackagesToSearchForDataTypes,
                seedClassesInPackagesToSearchForServices );
    }
    
    public static DatabaseSupport getSupport( final boolean useTestDataSource,
            final Set< Class< ? > > seedClassesInPackagesToSearchForDataTypes, 
            final Set< Class< ? > > seedClassesInPackagesToSearchForServices )
    {
        final DatabaseSupport retval = PROVIDER.generateCacheResultFor( 
                new DatabaseSupportInitParams( useTestDataSource,
                seedClassesInPackagesToSearchForDataTypes,
                seedClassesInPackagesToSearchForServices ) );
        
        if ( null == retval )
        {
            LOG.info( "Could not generate " + DatabaseSupport.class.getSimpleName() + ".  Returning null /"
                    + " the client requiring database support cannot have it." );
            throw new DatabaseSupportException( "Database support is unavailable." );
        }
        
        retval.reset();
        
        return retval;
    }
    
    
    public static void reset()
    {
        s_postgresExists = null;
        s_postgresUser = null;
    }
    
    
    private final static class DatabaseSupportInitParams
    {
        private DatabaseSupportInitParams( final boolean useTestDataSource,
            final Set< Class< ? > > seedClassesInPackagesToSearchForDataTypes, 
            final Set< Class< ? > > seedClassesInPackagesToSearchForServices )
        {
            m_useTestDataSource = useTestDataSource;
            m_seedClassesInPackagesToSearchForDataTypes = 
                    new HashSet<>( seedClassesInPackagesToSearchForDataTypes );
                    
            final Set<String> dtSet = new TreeSet<>();      // Must be ordered.
            for ( Class< ? > dt : m_seedClassesInPackagesToSearchForDataTypes )
            {
                dtSet.add( dt.getPackage().getName() );
            }
            final StringBuilder sb = new StringBuilder( 500 );
            for ( String pkg : dtSet )
            {
                sb.append( pkg );
            }
            m_dataTypePackages = sb.toString();
            
            m_seedClassesInPackagesToSearchForServices = 
                    new HashSet<>( seedClassesInPackagesToSearchForServices );
        }

        private final boolean m_useTestDataSource;
        private final Set< Class< ? > > m_seedClassesInPackagesToSearchForDataTypes;
        private final String m_dataTypePackages;
        private final Set< Class< ? > > m_seedClassesInPackagesToSearchForServices;
    }
    
    
    private final static class DatabaseSupportImpl implements DatabaseSupport
    {
        private DatabaseSupportImpl( 
                final String dbName,
                final boolean useTestDataSource,
                final DataSource dataSource,
                final DataManager dataManager,
                final Set< Class< ? > > seedClassesInPackagesToSearchForServices )
        {
            m_dbName = dbName;
            m_useTestDataSource = useTestDataSource;
            m_dataSource = dataSource;
            m_dataManager = dataManager;
            m_seedClassesInPackagesToSearchForServices = seedClassesInPackagesToSearchForServices;
        }

        public DataManager getDataManager()
        {
            return m_dataManager;
        }
        
        public DataSource getDataSource()
        {
            return m_dataSource;
        }
        
        public DataSource newDataSource()
        {
            return ( m_useTestDataSource ) ?
                     new TestDataSource( "localhost", m_dbName, DB_USER, DB_PASSWORD ) :
                     new PostgresDataSource( "localhost", m_dbName, DB_USER, DB_PASSWORD );
        }
        
        public BeansServiceManager getServiceManager()
        {
            if ( null == m_serviceManager )
            {
                throw new IllegalStateException( "Support has not been reset yet." );
            }
            return m_serviceManager;
        }
        
        public BasicTestsInvocationHandler getNotificationEventDispatcherBtih()
        {
            return m_nedBtih;
        }
        
        public void reset()
        {
            new DatabaseResetter( m_dataManager ).run();
            
            m_nedBtih = new BasicTestsInvocationHandler( MockInvocationHandler.forReturnType( 
                    NotificationEventDispatcher.class,
                    new InvocationHandler()
                    {
                        public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
                        {
                            return new TransactionNotificationEventDispatcher( 
                                    m_serviceManager.getNotificationEventDispatcher() );
                        }
                    },
                    MockInvocationHandler.forMethod(
                            ReflectUtil.getMethod( NotificationEventDispatcher.class, "commitTransaction" ),
                            new InvocationHandler()
                            {
                                public Object invoke( Object proxy, Method method, Object[] args )
                                        throws Throwable
                                {
                                    throw new IllegalStateException( "Cannot commit a non-transaction." );
                                }
                            },
                            null ) ) );
            try
            {
                m_serviceManager = SERVICE_MANAGER_CONSTRUCTOR.newInstance(
                        InterfaceProxyFactory.getProxy( NotificationEventDispatcher.class, m_nedBtih ),
                        null,
                        m_dataManager,
                        m_seedClassesInPackagesToSearchForServices,
                        Boolean.FALSE );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
        }
        
        
        public void executeSql( final File sqlFile )
        {
            final Duration duration = new Duration();
            LOG.info( "Executing sql file: " + sqlFile.getAbsolutePath() );
            final String url = "jdbc:postgresql://localhost/" + m_dbName;
            try ( Connection conn = DriverManager.getConnection( url, s_postgresUser, "" );
                  Statement stmt = conn.createStatement() )
            {
                final String sql = Files.readString( sqlFile.toPath() );
                stmt.execute( sql );
            }
            catch ( final IOException | SQLException ex )
            {
                throw new RuntimeException( "Failed to execute SQL file "
                        + sqlFile.getAbsolutePath(), ex );
            }
            LOG.info( "Executed sql file in " + duration + "." );
        }
        
        @Override
        protected void finalize() throws Throwable
        {
            m_dataManager.shutdown();
            super.finalize();
        }
        
        @Override
        public String getDbServerName()
        {
            return "localhost";
        }

        /**
         * @return
         */
        @Override
        public String getDbName()
        {
            return m_dbName;
        }

        @Override
        public String getDbUsername()
        {
            return DB_USER;
        }

        @Override
        public String getDbPassword()
        {
            return DB_PASSWORD;
        }
        
        private volatile BeansServiceManager m_serviceManager;
        private volatile BasicTestsInvocationHandler m_nedBtih;
        
        private final String m_dbName;
        private final boolean m_useTestDataSource;
        private final DataSource m_dataSource;
        private final DataManager m_dataManager;
        private final Set< Class< ? > > m_seedClassesInPackagesToSearchForServices;
        private final static Constructor< BeansServiceManagerImpl > SERVICE_MANAGER_CONSTRUCTOR;
        static
        {
            try
            {
                SERVICE_MANAGER_CONSTRUCTOR = 
                        BeansServiceManagerImpl.class.getDeclaredConstructor(
                                NotificationEventDispatcher.class,
                                BeansServiceManager.class,
                                DataManager.class,
                                Set.class,
                                boolean.class );
                SERVICE_MANAGER_CONSTRUCTOR.setAccessible( true );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
        }
    } // end inner class def
    
    
    private final static class DatabaseSupportProvider 
        implements CacheResultProvider< DatabaseSupportInitParams, DatabaseSupport >
    {
        synchronized public DatabaseSupport generateCacheResultFor( 
                final DatabaseSupportInitParams initParams )
        {
            if ( null == s_postgresExists )
            {
                for ( final String user : CollectionFactory.toList( "postgres", "Administrator" ) )
                {
                    if ( isValidConnection( user ) )
                    {
                        s_postgresExists = Boolean.TRUE;
                        s_postgresUser = user;

                        executeAdminSql( DEFAULT_DB,
                                "DO $$"
                                + "BEGIN"
                                + " IF NOT EXISTS("
                                + "   SELECT * FROM pg_catalog.pg_user WHERE usename = '" + DB_USER + "'"
                                + " )"
                                + " THEN"
                                + "   CREATE USER " + DB_USER + " WITH SUPERUSER PASSWORD '"
                                + DB_PASSWORD + "';"
                                + " END IF;"
                                + "END"
                                + "$$;" );
                        break;
                    }
                }
                if ( null == s_postgresExists )
                {
                    s_postgresExists = Boolean.FALSE;
                }
            }
            
            if ( !s_postgresExists.booleanValue() )
            {
                LOG.info( "Cannot find a Postgres SQL database server to connect to." );
                return null;
            }
            
            final PostgresDataManager dataManager = new PostgresDataManager( 8,
                  initParams.m_seedClassesInPackagesToSearchForDataTypes );
            String dbName = PKGS_TO_DB_NAME_MAP.get( initParams.m_dataTypePackages );
            if ( null == dbName )
            {
                final String sql = getDbInitSql( dataManager );
                final String md5 = DatabaseUtils.getChecksum( sql );
                dbName = DatabaseSupportFactory.class.getSimpleName().toLowerCase() + md5;
                validateUserDbConnection( dbName, dataManager );
                PKGS_TO_DB_NAME_MAP.put( initParams.m_dataTypePackages, dbName );
            }
            LOG.info( new StringBuilder().append( "Using DB " ).append( DB_USER )
                                         .append( ':' ).append(  dbName ) );
            
            final DataSource dataSource = ( initParams.m_useTestDataSource )?
                    new TestDataSource( "localhost", dbName, DB_USER, DB_PASSWORD ) :
                    new PostgresDataSource( "localhost", dbName, DB_USER, DB_PASSWORD );
            dataManager.setDataSource( dataSource );
            final DatabaseSupport retval = new DatabaseSupportImpl(
                    dbName,
                    initParams.m_useTestDataSource,
                    dataSource,
                    dataManager,
                    initParams.m_seedClassesInPackagesToSearchForServices );
            
            return retval;
        }
        
        
        private static synchronized String getDbInitSql(
                                                final DataManager dataManager )
        {
            final SqlCodeGenerator sqlCodeGenerator = new SqlCodeGenerator( 
                                    dataManager.getSupportedTypes(), DB_USER );
            return sqlCodeGenerator.getGeneratedCode().getCodeFiles().get( null );
        }
        

        private static synchronized void validateUserDbConnection(
                            final String dbName, final DataManager dataManager )
        {
            final String cacheKey =  new StringBuilder( 150 )
                          .append( s_postgresUser ).append( dbName ).toString();
            
            if ( USER_DB_CONN_VALIDATED_CACHE.contains( cacheKey ) )
            {
                return;
            }
            
            if ( !isValidConnection( s_postgresUser, dbName ) )
            {
                executeAdminSql( "template1", "DROP DATABASE IF EXISTS " + dbName );
                LOG.info( "Dropped database: " + dbName );

                final String dbCreateArgs;
                if ( System.getProperty("os.name").startsWith("Windows") )
                {
                    dbCreateArgs =
                    "template=template0 encoding='UTF8' lc_collate='C' lc_ctype='C'";
                }
                else // FreeBSD or Mac or Linux
                {
                    dbCreateArgs =
                    "template=template0 encoding='UTF8' lc_collate='C' lc_ctype='en_US.UTF-8'";
                }
                executeAdminSql( "template1",
                        "CREATE DATABASE " + dbName + " " + dbCreateArgs );
                LOG.info( "Created database: " + dbName );

                final String initSql = getDbInitSql( dataManager );
                LOG.info( "Initializing database " + dbName );
                LOG.debug( initSql );
                executeAdminSql( dbName, initSql );
            }
            USER_DB_CONN_VALIDATED_CACHE.add( cacheKey );
        }
        
        
        private static File generateTableSetupSqlFile( final String sqlCode )
        {
            try
            {
                final File retval = File.createTempFile(
                      DatabaseSupportProvider.class.getSimpleName() + "Creator", "sql" );
                final FileWriter writer = new FileWriter( retval );
                writer.write( sqlCode );
                writer.close();
                return retval;
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( "Failed to generate sql file.", ex );
            }
        }
        

        private boolean isValidConnection( final String username )
        {
            return isValidConnection( username, DEFAULT_DB );
        }


        private static boolean isValidConnection( final String username, final String dbName )
        {
            try ( Connection conn = connect( username, dbName ) )
            {
                return conn.isValid( 2 );
            }
            catch ( final SQLException ex )
            {
                LOG.info( "Failed to connect as " + username + " to " + dbName
                          + " (" + ex.toString() + ")" );
                return false;
            }
        }


        private static Connection connect( final String username, final String dbName )
                throws SQLException
        {
            final String url = "jdbc:postgresql://localhost/" + dbName;
            return DriverManager.getConnection( url, username, "" );
        }


        private static void executeAdminSql( final String dbName, final String sql )
        {
            final Duration duration = new Duration();
            LOG.info( "Running admin SQL on " + dbName );
            try ( Connection conn = connect( s_postgresUser, dbName );
                  Statement stmt = conn.createStatement() )
            {
                stmt.execute( sql );
                LOG.info( "Admin SQL completed in " + duration + "." );
            }
            catch ( final SQLException ex )
            {
                LOG.warn( "Failed to execute admin SQL on " + dbName
                          + ": " + ex.toString() );
            }
        }
    } // end inner class def
    
    
    private static Process execAndWaitFor( final String[] commandLine, final long timeoutInMillis )
    {
        final Process process;
        try
        {
            process = Runtime.getRuntime().exec( commandLine );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
        
        final CommandExecutionWorker worker = new CommandExecutionWorker( process );
        worker.start();
        try 
        {
            process.getOutputStream().close();
            
            final BufferedReader stdin = new BufferedReader(new InputStreamReader(process.getInputStream()));
            SystemWorkPool.getInstance().submit( new StreamReaderWorker( stdin ) );

            final BufferedReader stderr = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            SystemWorkPool.getInstance().submit( new StreamReaderWorker( stderr ) );

            worker.join( timeoutInMillis );
            if ( worker.m_returned )
            {
                return process;
            }
            
            process.destroy();
            
            throw new RuntimeException( "Timed out after " + timeoutInMillis + "ms." );
        } 
        catch ( final Exception ex ) 
        {
            worker.interrupt();
            throw new RuntimeException( ex );
        } 
        finally 
        {
            process.destroy();
        }
    } 

    
    private final static class CommandExecutionWorker extends Thread 
    {
        private CommandExecutionWorker( final Process process ) 
        {
            m_process = process;
        }
        
        @Override
        public void run()
        {
            try
            { 
                m_process.waitFor();
                m_returned = true;
            } 
            catch ( final InterruptedException ex ) 
            {
                throw new RuntimeException( ex );
            }
        }  
        private final Process m_process;
        private volatile boolean m_returned;
    } // end inner class def
    
    
    private final static class StreamReaderWorker implements Runnable
    {
        private StreamReaderWorker( final BufferedReader reader )
        {
            m_reader = reader;
        }

        public void run()
        {
            try
            {
                String line = "";
                while ( null != line )
                {
                    line = m_reader.readLine();
                }
            }
            catch ( final IOException ex )
            {
                LOG.debug( "Failed to read from stream.", ex );
            }
        }
        private final BufferedReader m_reader;
    } // end inner class def
    
    
    private final static DatabaseSupportProvider PROVIDER = new DatabaseSupportProvider();
    public static final String PSQL = "psql";
    
    private final static Set<String> USER_DB_CONN_VALIDATED_CACHE =
                                                               new HashSet< >();
    
    private final static Map<String, String> PKGS_TO_DB_NAME_MAP =
                   Collections.synchronizedMap( new HashMap<String, String>() );
            
    private final static String DB_USER =
             NamingConventionType.UNDERSCORED.convert( 
                DatabaseSupportFactory.class.getSimpleName() + "DatabaseUser" );
    
    private final static String DB_PASSWORD = "passw0rd";
    private final static String DEFAULT_DB = "template1";
    
    private static Boolean s_postgresExists;
    private static String s_postgresUser;
    
    private final static Logger LOG = Logger.getLogger( DatabaseSupportFactory.class );
    
    static
    {
        try
        {
            Field f = ConnectionPoolFactory.class.getDeclaredField(
                    ConnectionPoolFactory.USE_TEST_CONN_POOL_FIELD );
            f.setAccessible( true );
            ((AtomicBoolean)f.get( (ConnectionPoolFactory)null )).set( true );
            
            LOG.info( ConnectionPoolFactory.class.getSimpleName() + " will use " +
                      TestConnectionPool.class.getSimpleName() + " as default " +
                                           ConnectionPool.class.getSimpleName() );
            
            f = DataSourceValidator.class.getDeclaredField(
                   DataSourceValidator.ALRREADY_VALIDATED_CACHE_ENABLED_FIELD );
            f.setAccessible( true );
            ((AtomicBoolean)f.get( (DataSourceValidator)null )).set( true );
            
            LOG.info( DataSourceValidator.class.getSimpleName() + " will use " +
                 " its backing-DB-table-already-validated performance cache." );
        }
        catch ( final NoSuchFieldException     | SecurityException |
                      IllegalArgumentException | IllegalAccessException ex )
        {
            LOG.error( ex );
            throw new RuntimeException( ex );
        }
    }
}
