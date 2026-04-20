/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.performancetest;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.db.manager.frmwrk.DefaultConnectionPool;
import com.spectralogic.util.db.mockdomain.County;
import com.spectralogic.util.db.mockdomain.Teacher;
import com.spectralogic.util.db.mockdomain.TeacherType;
import com.spectralogic.util.db.mockservice.CountyService;
import com.spectralogic.util.db.query.Query;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;

public final class DatabaseIntegration_Test extends TestCase
{

    public void testNonTransactionPerformance()
    {
        DatabaseSupportFactory.reset();
        final DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(
                                    false, Teacher.class, CountyService.class );
        final Level originalLevel = Logger.getLogger( DefaultConnectionPool.class.getName() ).getLevel();
        try
        {
            Logger.getLogger( DefaultConnectionPool.class.getName() ).setLevel( Level.WARN );
            loadDatabase( dbSupport );
            
            for ( final Integer tc : THREAD_COUNTS_TO_TEST )
            {
                performConcurrentCountPerformanceTest( tc.intValue(), false, dbSupport );
            }
            for ( final Integer tc : THREAD_COUNTS_TO_TEST )
            {
                performConcurrentGetPerformanceTest( tc.intValue(), false, dbSupport );
            }
            for ( final Integer tc : THREAD_COUNTS_TO_TEST )
            {
                performConcurrentCreatePerformanceTest( tc.intValue(), false, dbSupport );
            }
            for ( final Integer tc : THREAD_COUNTS_TO_TEST )
            {
                performConcurrentUpdateSingleBeanPerformanceTest( tc.intValue(), false,
                        dbSupport );
            }
            for ( final Integer tc : THREAD_COUNTS_TO_TEST )
            {
                performConcurrentUpdateMultipleBeansPerformanceTest( tc.intValue(), false, dbSupport );
            }
            for ( final Integer tc : THREAD_COUNTS_TO_TEST )
            {
                performConcurrentDeleteSingleBeanPerformanceTest( tc.intValue(), false, dbSupport );
            }
        }
        finally
        {
            Logger.getLogger( DefaultConnectionPool.class.getName() ).setLevel( originalLevel );
            dbSupport.getDataManager().shutdown();
        }
    }


    public void testTransactionPerformance()
    {        
        DatabaseSupportFactory.reset();
        final DatabaseSupport dbSupport =  DatabaseSupportFactory.getSupport(
                                    false, Teacher.class, CountyService.class );
        final Level originalLevel = Logger.getLogger( DefaultConnectionPool.class.getName() ).getLevel();
        try
        {
            Logger.getLogger( DefaultConnectionPool.class.getName() ).setLevel( Level.WARN );
            loadDatabase( dbSupport );
            
            /*
            for ( final Integer tc : THREAD_COUNTS_TO_TEST )
            {   
                performConcurrentCountPerformanceTest( tc.intValue(), true, dbSupport );
            }
            for ( final Integer tc : THREAD_COUNTS_TO_TEST )
            {
                performConcurrentGetPerformanceTest( tc.intValue(), true, dbSupport );
            }
            */
            /*
            for ( final Integer tc : THREAD_COUNTS_TO_TEST )
            {
                performConcurrentCreatePerformanceTest( tc.intValue(), true, dbSupport );
            }
            */
            /*
            for ( final Integer tc : THREAD_COUNTS_TO_TEST )
            {
                performConcurrentUpdateSingleBeanPerformanceTest( tc.intValue(), true, dbSupport );
            }
            for ( final Integer tc : THREAD_COUNTS_TO_TEST )
            {
                performConcurrentUpdateMultipleBeansPerformanceTest( tc.intValue(), true, dbSupport );
            }
            */
            for ( final Integer tc : THREAD_COUNTS_TO_TEST )
            {
                performConcurrentDeleteSingleBeanPerformanceTest( tc.intValue(), true, dbSupport );
            }
        }
        finally
        {
            Logger.getLogger( DefaultConnectionPool.class.getName() ).setLevel( originalLevel );
            dbSupport.getDataManager().shutdown();
        }
    }
    
    

    private void loadDatabase( final DatabaseSupport dbSupport )
    {
        final File sqlFile = loadDatabaseInternal();
        sqlFile.deleteOnExit();
        dbSupport.executeSql( sqlFile );
        sqlFile.delete();
        
        final Set< County > counties = dbSupport.getDataManager()
                .getBeans( County.class, Query.where( Require.nothing() ) )
                .toSet();
        assertEquals(
                "Shoulda created counties.",
                NUMBER_OF_COUNTIES,
                counties.size() );
        LOG.info( "Loaded database with " + NUMBER_OF_COUNTIES + " counties." );
        
        final Set< UUID > countyIds = new HashSet<>();
        for ( final County c : counties )
        {
            countyIds.add( c.getId() );
        }
        countyIds.remove( countyIds.iterator().next() );
        
        final Set< County > countiesRetrievedByIds = dbSupport.getServiceManager().getService( 
                CountyService.class ).retrieveAll( countyIds ).toSet();
        assertEquals(
                "Shoulda found counties.",
                NUMBER_OF_COUNTIES - 1,
                countiesRetrievedByIds.size() );
    }
    
    
    private File loadDatabaseInternal()
    {
        try
        {
            final File retval = File.createTempFile( getClass().getSimpleName(), "sql" );
            final FileWriter writer = new FileWriter( retval );
            
            writer.write( "DELETE FROM mockdomain.county ; " );
            writer.write( "INSERT INTO mockdomain.county (id, name, population) VALUES " );
            for ( int i = 0; i < NUMBER_OF_COUNTIES; ++i )
            {
                if ( 0 < i )
                {
                    writer.write( "," );
                }
                writer.write(
                        "('" + UUID.randomUUID().toString() + "'," + "'county" + i + "'," + i + ")" );
            }
            
            writer.close();
            return retval;
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( "Failed to generate sql file.", ex );
        }
    }

    
    private void performConcurrentCountPerformanceTest( final int numThreads, final boolean useTransactions,
            final DatabaseSupport dbSupport )
    {
        final Duration duration = new Duration();
        final AtomicInteger numberOfBeansRetrieved = new AtomicInteger();
        final WorkPool wp = WorkPoolFactory.createWorkPool( numThreads, getClass().getSimpleName() );
        final AtomicBoolean continueRunning = new AtomicBoolean( true );
        final int sliceSize = NUMBER_OF_COUNTIES / 100;
        for ( int t = 0; t < numThreads; ++t )
        {
            final int min = t * sliceSize;
            final int max = ( t + 1 ) * sliceSize;
            wp.submit( new Runnable()
            {
                public void run()
                {
                    final DataManagerProvider dmp = new DataManagerProvider( useTransactions,
                                                                             dbSupport );
                    while ( continueRunning.get() )
                    {
                        final int numBeans = dmp.getDataManager().getCount(
                                County.class,
                                Require.all( Require.beanPropertyLessThan( 
                                                County.POPULATION, Integer.valueOf( max ) ),
                                             Require.beanPropertyGreaterThan(
                                                County.POPULATION, Integer.valueOf( min ) ) ) );
                        numberOfBeansRetrieved.addAndGet( numBeans );
                    }
                    dmp.done();
                }
            } );
        }
        try
        {
            Thread.sleep( 1000 );
            continueRunning.set( false );
            assertTrue(
              "Shoulda been more than 0 beans retrieved. Thread count: " +
                                 numThreads, 0 < numberOfBeansRetrieved.get() );
            LOG.info( Platform.NEWLINE + Platform.NEWLINE + Platform.NEWLINE +
                 "With " + numThreads + " threads, counting beans at " 
                       + ( numberOfBeansRetrieved.get() / 1 ) + " / sec" +
                     Platform.NEWLINE + Platform.NEWLINE + Platform.NEWLINE );
        }
        catch ( final InterruptedException ex )
        {
            throw new RuntimeException( ex );
        }
        finally
        {
            wp.shutdown();
            try
            {
                wp.awaitTermination( 30, TimeUnit.SECONDS );
            }
            catch ( final InterruptedException ex )
            {
                throw new RuntimeException( ex );
            }
        }

        if ( 2 < duration.getElapsedSeconds() )
        {
            LOG.info( "Count test with " + numThreads + " threads completed in " + duration + "." );
        }
    }
    
    
    private void performConcurrentGetPerformanceTest( final int numThreads, final boolean useTransactions,
            final DatabaseSupport dbSupport )
    {
        final Duration duration = new Duration();
        final AtomicInteger numberOfBeansRetrieved = new AtomicInteger();
        final WorkPool wp = WorkPoolFactory.createWorkPool( numThreads, getClass().getSimpleName() );
        final AtomicBoolean continueRunning = new AtomicBoolean( true );
        final int sliceSize = NUMBER_OF_COUNTIES / 100;
        for ( int t = 0; t < numThreads; ++t )
        {
            final int min = t * sliceSize;
            final int max = ( t + 1 ) * sliceSize;
            wp.submit( new Runnable()
            {
                public void run()
                {
                    final DataManagerProvider dmp = new DataManagerProvider( useTransactions,
                                                                             dbSupport );
                    while ( continueRunning.get() )
                    {
                        final Set< County > beans = dmp.getDataManager().getBeans(
                                County.class,
                                Query.where( Require.all( Require.beanPropertyLessThan( 
                                                County.POPULATION, Integer.valueOf( max ) ),
                                             Require.beanPropertyGreaterThan(
                                                County.POPULATION, Integer.valueOf( min ) ) ) ) ).toSet();
                        if ( beans.isEmpty() )
                        {
                            throw new IllegalStateException(
                                    "'beans' set is not allowed to be empty." );
                        }
                        numberOfBeansRetrieved.addAndGet( beans.size() );
                    }
                    dmp.done();
                }
            } );
        }
        try
        {
            Thread.sleep( 1000 );
            continueRunning.set( false );
            assertTrue(
                "Shoulda been more than 0 beans retrieved. Thread count: " +
                                 numThreads, 0 < numberOfBeansRetrieved.get() );
            LOG.info( Platform.NEWLINE + Platform.NEWLINE + Platform.NEWLINE +
                      "With " + numThreads + " threads, getting beans at " 
                       + ( numberOfBeansRetrieved.get() / 1 ) + " / sec" +
                       Platform.NEWLINE + Platform.NEWLINE + Platform.NEWLINE );
        }
        catch ( final InterruptedException ex )
        {
            throw new RuntimeException( ex );
        }
        finally
        {
            wp.shutdown();
            try
            {
                wp.awaitTermination( 30, TimeUnit.SECONDS );
            }
            catch ( final InterruptedException ex )
            {
                throw new RuntimeException( ex );
            }
        }

        if ( 2 < duration.getElapsedSeconds() )
        {
            LOG.info( "Get test with " + numThreads + " threads completed in " + duration + "." );
        }
    }
    
    
    private void performConcurrentUpdateSingleBeanPerformanceTest(
            final int numThreads, final boolean useTransactions,
            final DatabaseSupport dbSupport )
    {
        final Duration duration = new Duration();
        final AtomicInteger numberOfBeansUpdated = new AtomicInteger();
        final WorkPool wp = WorkPoolFactory.createWorkPool( numThreads, getClass().getSimpleName() );
        final AtomicBoolean continueRunning = new AtomicBoolean( true );
        final int sliceSize = NUMBER_OF_COUNTIES / 100;
        final CountDownLatch readyLatch = new CountDownLatch( numThreads );
        final CountDownLatch startLatch = new CountDownLatch( 1 );
        for ( int t = 0; t < numThreads; ++t )
        {
            final int min = t * sliceSize;
            final int max = ( t + 1 ) * sliceSize;
            wp.submit( new Runnable()
            {
                public void run()
                {
                    final Set< County > beans = dbSupport.getDataManager().getBeans(
                            County.class,
                            Query.where( Require.all( Require.beanPropertyLessThan( 
                                            County.POPULATION, Integer.valueOf( max ) ),
                                         Require.beanPropertyGreaterThan(
                                            County.POPULATION, Integer.valueOf( min ) ) ) ) ).toSet();
                    try
                    {
                        readyLatch.countDown();
                        startLatch.await();
                    }
                    catch ( final InterruptedException ex )
                    {
                        throw new RuntimeException( ex );
                    }

                    if ( beans.isEmpty() )
                    {
                        throw new IllegalStateException(
                                    "'beans' set is not allowed to be empty." );
                    }
                    
                    final DataManagerProvider dmp = new DataManagerProvider( useTransactions,
                                                                             dbSupport );
                    while ( true )
                    {
                        for ( final County bean : beans )
                        {
                            bean.setName( String.valueOf( System.nanoTime() ) );
                            dmp.getDataManager().updateBean( null, bean );
                            numberOfBeansUpdated.addAndGet( 1 );
                            
                            if ( !continueRunning.get() )
                            {
                                dmp.done();
                                return;
                            }
                        }
                    }
                }
            } );
        }
        
        
        try
        {
            readyLatch.await();
            startLatch.countDown();
            Thread.sleep( 1000 );
            continueRunning.set( false );
            assertTrue(
                 "Shoulda been more than 0 beans updated. Thread count: " +
                                   numThreads, 0 < numberOfBeansUpdated.get() );
            LOG.info( Platform.NEWLINE + Platform.NEWLINE + Platform.NEWLINE +
                     "With " + numThreads + " threads, updating beans individually at " 
                       + ( numberOfBeansUpdated.get() / 1 ) + " / sec" + 
                       Platform.NEWLINE + Platform.NEWLINE + Platform.NEWLINE );
        }
        catch ( final InterruptedException ex )
        {
            throw new RuntimeException( ex );
        }
        finally
        {
            wp.shutdown();
            try
            {
                wp.awaitTermination( 30, TimeUnit.SECONDS );
            }
            catch ( final InterruptedException ex )
            {
                throw new RuntimeException( ex );
            }
        }

        if ( 2 < duration.getElapsedSeconds() )
        {
            LOG.info( "Update test with " + numThreads + " threads completed in " + duration + "." );
        }
    }
    
    

    private void performConcurrentUpdateMultipleBeansPerformanceTest(
            final int numThreads, final boolean useTransactions,
            final DatabaseSupport dbSupport )
    {
        final Duration duration = new Duration();
        final AtomicInteger numberOfBeansUpdated = new AtomicInteger();
        final WorkPool wp = WorkPoolFactory.createWorkPool( numThreads, getClass().getSimpleName() );
        final AtomicBoolean continueRunning = new AtomicBoolean( true );
        final int sliceSize = NUMBER_OF_COUNTIES / 100;
        final CountDownLatch readyLatch = new CountDownLatch( numThreads );
        final CountDownLatch startLatch = new CountDownLatch( 1 );
        for ( int t = 0; t < numThreads; ++t )
        {
            final int min = t * sliceSize;
            final int max = ( t + 1 ) * sliceSize;
            wp.submit( new Runnable()
            {
                public void run()
                {
                    final Set< County > beans = dbSupport.getDataManager().getBeans(
                            County.class,
                            Query.where( Require.all( Require.beanPropertyLessThan( 
                                            County.POPULATION, Integer.valueOf( max ) ),
                                         Require.beanPropertyGreaterThan(
                                            County.POPULATION, Integer.valueOf( min ) ) ) ) ).toSet();
                    try
                    {
                        readyLatch.countDown();
                        startLatch.await();
                    }
                    catch ( final InterruptedException ex )
                    {
                        throw new RuntimeException( ex );
                    }

                    if ( beans.isEmpty() )
                    {
                        throw new IllegalStateException(
                                "'beans' set is not allowed to be empty." );
                    }
                    
                    final DataManagerProvider dmp = new DataManagerProvider( useTransactions,
                                                                             dbSupport );
                    while ( continueRunning.get() )
                    {
                        final County bean = BeanFactory.newBean( County.class );
                        bean.setPopulation( 50L );
                        dmp.getDataManager().updateBeans(
                                CollectionFactory.toSet( County.POPULATION ),
                                bean, 
                                Require.all( Require.beanPropertyLessThan( 
                                                County.POPULATION, Integer.valueOf( max ) ),
                                             Require.beanPropertyGreaterThan(
                                                County.POPULATION, Integer.valueOf( min ) ) ) );
                        numberOfBeansUpdated.addAndGet( beans.size() );
                    }
                    dmp.done();
                }
            } );
        }
        
        try
        {
            readyLatch.await();
            startLatch.countDown();
            Thread.sleep( 1000 );
            continueRunning.set( false );
            assertTrue( 
                  "Shoulda been more than 0 beans updated. Thread count" +
                                  numThreads, 0 < numberOfBeansUpdated.get() );
            LOG.info( Platform.NEWLINE + Platform.NEWLINE + Platform.NEWLINE +
              "With " + numThreads + " threads, updating beans " + sliceSize + " at a time at " 
                       + ( numberOfBeansUpdated.get() / 1 ) + " / sec" +
                       Platform.NEWLINE + Platform.NEWLINE + Platform.NEWLINE );
        }
        catch ( final InterruptedException ex )
        {
            throw new RuntimeException( ex );
        }
        finally
        {
            wp.shutdown();
            try
            {
                wp.awaitTermination( 30, TimeUnit.SECONDS );
            }
            catch ( final InterruptedException ex )
            {
                throw new RuntimeException( ex );
            }
        }
        
        if ( 2 < duration.getElapsedSeconds() )
        {
            LOG.info( "Bulk update test with " + numThreads + " threads completed in " + duration + "." );
        }
    }
    
    
    private void performConcurrentCreatePerformanceTest( final int numThreads, final boolean useTransactions,
            final DatabaseSupport dbSupport )
    {
        final AtomicInteger i = new AtomicInteger();
        final WorkPool wp = WorkPoolFactory.createWorkPool( numThreads, getClass().getSimpleName() );
        final Duration duration = new Duration();
        for ( int t = 0; t < numThreads; ++t )
        {
            wp.submit( new Runnable()
            {
                public void run()
                {
                    final DataManagerProvider dmp = new DataManagerProvider( useTransactions,
                                                                             dbSupport );
                    while ( 1 > duration.getElapsedSeconds() )
                    {
                        final int j = i.addAndGet( 1 );
                        final Teacher bean = BeanFactory.newBean( Teacher.class );
                        bean.setComments( "Here are some comments that go on and on for a bit " + j );
                        bean.setName( System.nanoTime() + " name " + j );
                        bean.setYearsOfService( j );
                        bean.setType( TeacherType.TEACHER );
                        bean.setDateOfBirth( new Date() );
                        dmp.getDataManager().createBean( bean );
                    }
                    dmp.done();
                }
            } );
        }
        
        try
        {
            Thread.sleep( 1000 );
        }
        catch ( final InterruptedException ex )
        {
            throw new RuntimeException( ex );
        }
        
        assertTrue(
             "Shoulda been more than 0 beans created. Thread count: " + 
                                                     numThreads, 0 < i.get() );
        LOG.info( Platform.NEWLINE + Platform.NEWLINE + Platform.NEWLINE + 
           "With " + numThreads + " threads, creating beans at " + i.get() + " / sec" + 
                       Platform.NEWLINE + Platform.NEWLINE + Platform.NEWLINE );
        wp.shutdown();
        try
        {
            wp.awaitTermination( 30, TimeUnit.SECONDS );
        }
        catch ( final InterruptedException ex )
        {
            throw new RuntimeException( ex );
        }
        
        if ( 2 < duration.getElapsedSeconds() )
        {
            LOG.info( "Create test with " + numThreads + " threads completed in " + duration + "." );
        }
    }
    
    
    
    private void performConcurrentDeleteSingleBeanPerformanceTest(
            final int numThreads, final boolean useTransactions,
            final DatabaseSupport dbSupport )
    {
        // Sometimes the first few calls of this with less numbers of threads
        // deletes so many beans that when getting to the bigger thread counts
        // there's no beans left to delete, so:
        loadDatabase( dbSupport );
        
        final Duration duration = new Duration();
        final AtomicInteger numberOfBeansUpdated = new AtomicInteger();
        final WorkPool wp = WorkPoolFactory.createWorkPool( numThreads, getClass().getSimpleName() );
        final AtomicBoolean continueRunning = new AtomicBoolean( true );
        final int sliceSize = NUMBER_OF_COUNTIES / numThreads;
        final CountDownLatch readyLatch = new CountDownLatch( numThreads );
        final CountDownLatch startLatch = new CountDownLatch( 1 );
        for ( int t = 0; t < numThreads; ++t )
        {
            final int min = t * sliceSize;
            final int max = ( t + 1 ) * sliceSize;
            wp.submit( new Runnable()
            {
                public void run()
                {
                    final Set< County > beans = dbSupport.getDataManager().getBeans(
                            County.class,
                            Query.where( Require.all( Require.beanPropertyLessThan( 
                                            County.POPULATION, Integer.valueOf( max ) ),
                                         Require.beanPropertyGreaterThan(
                                            County.POPULATION, Integer.valueOf( min ) ) ) ) ).toSet();
                    try
                    {
                        readyLatch.countDown();
                        startLatch.await();
                    }
                    catch ( final InterruptedException ex )
                    {
                        throw new RuntimeException( ex );
                    }

                    if ( beans.isEmpty() )
                    {
                        throw new IllegalStateException(
                                "'beans' set is not allowed to be empty." );
                    }
                    
                    final DataManagerProvider dmp = new DataManagerProvider( useTransactions,
                                                                             dbSupport );
                    for ( final County bean : beans )
                    {
                        dmp.getDataManager().deleteBean( County.class, bean.getId() );
                        numberOfBeansUpdated.addAndGet( 1 );
                        
                        if ( !continueRunning.get() )
                        {
                            dmp.done();
                            return;
                        }
                    }
                    LOG.warn( "Delete thread ran out of work.  Performance results may be inaccurate." );
                    dmp.done();
                }
            } );
        }
        
        try
        {
            readyLatch.await();
            startLatch.countDown();
            Thread.sleep( 500 );
            continueRunning.set( false );
            assertTrue(
               "Shoulda been more than 0 beans deleted. Thread count: " +
                                   numThreads, 0 < numberOfBeansUpdated.get() );
            LOG.info( Platform.NEWLINE + Platform.NEWLINE + Platform.NEWLINE +
                 "With " + numThreads + " threads, deleting beans individually at " 
                       + ( numberOfBeansUpdated.get() * 2 ) + " / sec" +
                       Platform.NEWLINE + Platform.NEWLINE + Platform.NEWLINE );
        }
        catch ( final InterruptedException ex )
        {
            throw new RuntimeException( ex );
        }
        finally
        {
            wp.shutdown();
            try
            {
                wp.awaitTermination( 30, TimeUnit.SECONDS );
            }
            catch ( final InterruptedException ex )
            {
                throw new RuntimeException( ex );
            }
        }
        
        if ( 2 < duration.getElapsedSeconds() )
        {
            LOG.info( "Delete test with " + numThreads + " threads completed in " + duration + "." );
        }
    }
    
    private final static class DataManagerProvider
    {
        private DataManagerProvider( final boolean useTransactions,
                                     final DatabaseSupport dbSupport )
        {
            m_useTransactions = useTransactions;
            m_dbSupport = dbSupport;
        }
        
        
        private DataManager getDataManager()
        {
            if ( !m_useTransactions )
            {
                return m_dbSupport.getDataManager();
            }
            if ( null == m_dm || 50 == ++m_callCount )
            {
                m_callCount = 0;
                if ( null != m_dm )
                {
                    m_dm.commitTransaction();
                }
                m_dm = m_dbSupport.getDataManager().startTransaction();
            }
            return m_dm;
        }
        
        
        private void done()
        {
            if ( null != m_dm )
            {
                m_dm.commitTransaction();
            }
        }
        
        
        private final boolean m_useTransactions;
        private final DatabaseSupport m_dbSupport;
        private DataManager m_dm;
        private int m_callCount;
    } // end inner class def
    
    private final static int NUMBER_OF_COUNTIES = 10000;
    private final static List< Integer > THREAD_COUNTS_TO_TEST;
    private final static Logger LOG = Logger.getLogger( DatabaseIntegration_Test.class );
    static
    {
        THREAD_COUNTS_TO_TEST = new ArrayList<>();
        THREAD_COUNTS_TO_TEST.add( Integer.valueOf( 1 ) );
        THREAD_COUNTS_TO_TEST.add( Integer.valueOf( 5 ) );
        THREAD_COUNTS_TO_TEST.add( Integer.valueOf( 25 ) );
        THREAD_COUNTS_TO_TEST.add( Integer.valueOf( 100 ) );
    }
}
