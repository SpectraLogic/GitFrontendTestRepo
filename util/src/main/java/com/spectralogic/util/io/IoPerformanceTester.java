/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.render.BytesRenderer;
import com.spectralogic.util.thread.wp.SystemWorkPool;

/**
 * Tests a particular file path for performance
 */
public final class IoPerformanceTester implements Runnable
{
    public IoPerformanceTester( final String directory, final int testTimeInMillis )
    {
        m_directory = new File( directory.endsWith( Platform.FILE_SEPARATOR ) ?
                directory.substring( 0, directory.length() - 1 ) 
                : directory );
        m_testTimeInMillis = testTimeInMillis;
        
        if ( !m_directory.exists() )
        {
            throw new IllegalArgumentException( "Does not exist: " + directory );
        }
    }
    
    
    public void run()
    {
        test( 1 );
        test( 2 );
        test( 4 );
    }
    
    
    private void test( final int numberOfThreads )
    {
        final Set< Tester > testers = new HashSet<>();
        for ( int i = 0; i < numberOfThreads; ++i )
        {
            final Tester tester = new Tester( i );
            testers.add( tester );
            SystemWorkPool.getInstance().submit( tester );
        }
        
        final List< Long > writeBytesPerSec = new ArrayList<>();
        final List< Long > readBytesPerSec = new ArrayList<>();
        for ( final Tester t : testers )
        {
            try
            {
                t.m_doneLatch.await();
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
            
            writeBytesPerSec.add( 
                    Long.valueOf( t.m_writtenBytes * 1000 / Math.max( 1, m_testTimeInMillis ) ) );
            readBytesPerSec.add( 
                    Long.valueOf( t.m_writtenBytes * 1000 / Math.max( 1, t.m_millisToRead ) ) );
        }
        
        Collections.sort( writeBytesPerSec );
        Collections.sort( readBytesPerSec );
        
        String writePerformance = "";
        String readPerformance = "";
        final BytesRenderer bytesRenderer = new BytesRenderer();
        for ( final Long bps : writeBytesPerSec )
        {
            if ( !writePerformance.isEmpty() )
            {
                writePerformance += ", ";
            }
            writePerformance += bytesRenderer.render( bps.longValue() ) + "/sec";
        }
        for ( final Long bps : readBytesPerSec )
        {
            if ( !readPerformance.isEmpty() )
            {
                readPerformance += ", ";
            }
            readPerformance += bytesRenderer.render( bps.longValue() ) + "/sec";
        }

        LOG.info( "I/O performance of " + m_directory + " with " + numberOfThreads + " concurrent threads:" );
        LOG.info( " -> Write performance: " + writePerformance );
        LOG.info( " -> Read performance: " + readPerformance );
    }
    
    
    private final class Tester implements Runnable
    {
        private Tester( final int threadNumber )
        {
            m_file = new File( m_directory.getAbsolutePath() + Platform.FILE_SEPARATOR 
                               + getClass().getSimpleName() + threadNumber );
        }
        
        
        public void run()
        {
            try
            {
                m_writtenBytes = testWritePerformance();
                m_millisToRead = testReadPerformance();
                m_doneLatch.countDown();
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
        }
        
        
        /**
         * @return number of bytes written in the test time in millis provided
         */
        private long testWritePerformance() throws IOException
        {
            final Duration duration = new Duration();
            final FileOutputStream out = new FileOutputStream( m_file );
            
            long retval = 0;
            final byte [] buffer = new byte[ 8 * 1024 ];
            for ( int i = 0; i < buffer.length; ++i )
            {
                buffer[ i ] = (byte)( i % 200 );
            }
            
            while ( m_testTimeInMillis > duration.getElapsedMillis() )
            {
                out.write( buffer );
                retval += buffer.length;
            }
            
            out.close();
            
            return retval;
        }
        
        
        /**
         * @return number of millis to read all the bytes written from the write performance test
         */
        private long testReadPerformance() throws IOException
        {
            final Duration duration = new Duration();
            final FileInputStream in = new FileInputStream( m_file );
            final byte [] buffer = new byte[ 8 * 1024 ];
            int length = 1;
            while ( 0 < length )
            {
                length = in.read( buffer );
            }
            
            in.close();
            m_file.delete();
            return duration.getElapsedMillis();
        }
        
        
        private volatile long m_writtenBytes;
        private volatile long m_millisToRead;
        private final File m_file;
        private final CountDownLatch m_doneLatch = new CountDownLatch( 1 );
    } // end inner class def
    
    
    private final File m_directory;
    private final int m_testTimeInMillis;
    
    private final static Logger LOG = Logger.getLogger( IoPerformanceTester.class );
}
