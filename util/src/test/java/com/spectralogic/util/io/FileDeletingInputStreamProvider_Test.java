/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.SystemWorkPool;

public final class FileDeletingInputStreamProvider_Test 
{
    @Test
    public void testConstructorNullInputStreamNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new FileDeletingInputStreamProvider( null );
            }
        } );
    }
    
    
    @Test
    public void testConstructorEmptyInputStreamNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new FileDeletingInputStreamProvider( new ArrayList< File >() );
            }
        } );
    }
    
    
    @Test
    public void testGetNextInputStreamReturnsAppropriately() throws IOException
    {
        final int fileCount = 3;
        final List< File > files = new ArrayList<>();
        for ( int i = 0; i < fileCount; ++i )
        {
            final File file = File.createTempFile( getClass().getSimpleName(), "tempfile" + i );
            file.deleteOnExit();
            files.add( file );
        }
        
        final FileDeletingInputStreamProvider provider = new FileDeletingInputStreamProvider( files );
        for ( int i = 0; i < fileCount; ++i )
        {
            assertNotNull(
                    provider.getNextInputStream(),
                    "Shoulda returned next file."
                    );
            for ( int j = 0; j < i; ++j )
            {
                assertFalse(files.get( j ).exists(), "Shoulda deleted consumed file: " + files.get( j ));
            }
            for ( int j = i; j < fileCount; ++j )
            {
                assertTrue(
                        files.get( j ).exists(),
                        "Should notta deleted file yet."
                         );
            }
        }
        
        assertNull(
                provider.getNextInputStream(),
                "Should notta been any files left."
                 );
        for ( final File file : files )
        {
            assertFalse(file.exists(), "Shoulda deleted consumed file.");
        }
    }
    
    
    @Test
    public void testConcurrentUsageWorks() throws InterruptedException, ExecutionException, TimeoutException
    {
        final byte [] data = new byte[ 1024 * 1024 ];
        for ( int i = 0; i < data.length; ++i )
        {
            data[ i ] = (byte)( i % 100 );
        }
        
        final int numThreads = 10;
        final Set< Future< ? > > futures = new HashSet<>();
        for ( int tNum = 0; tNum < numThreads; ++tNum )
        {
            final int finalThreadNum = tNum;
            futures.add( SystemWorkPool.getInstance().submit( new Runnable()
            {
                public void run()
                {
                    final int fileCount = 3;
                    final List< File > files = new ArrayList<>();
                    for ( int i = 0; i < fileCount; ++i )
                    {
                        try
                        {
                            final File file = File.createTempFile(
                                    FileDeletingInputStreamProvider_Test.class.getSimpleName(),
                                    "thread" + finalThreadNum + "tempfile" + i );
                            file.deleteOnExit();
                            final FileOutputStream out = new FileOutputStream( file );
                            out.write( data );
                            out.close();
                            files.add( file );
                        }
                        catch ( final Exception ex )
                        {
                            throw new RuntimeException( ex );
                        }
                    }
                    
                    final FileDeletingInputStreamProvider provider = 
                            new FileDeletingInputStreamProvider( files );
                    for ( int i = 0; i < fileCount; ++i )
                    {
                        assertNotNull(
                                provider.getNextInputStream(),
                                "Shoulda returned next file."
                                 );
                        for ( int j = 0; j < i; ++j )
                        {
                            assertFalse(files.get( j ).exists(), "Shoulda deleted consumed file: " + files.get( j ));
                        }
                        for ( int j = i; j < fileCount; ++j )
                        {
                            assertTrue(
                                    files.get( j ).exists(),
                                    "Should notta deleted file yet."
                                     );
                        }
                    }
                    
                    assertNull(
                            provider.getNextInputStream(),
                            "Should notta been any files left."
                             );
                    for ( final File file : files )
                    {
                        assertFalse(file.exists(), "Shoulda deleted consumed file.");
                    }
                }
            } ) );
        }
        
        for ( final Future< ? > future : futures )
        {
            future.get( 10, TimeUnit.SECONDS );
        }
    }
}
