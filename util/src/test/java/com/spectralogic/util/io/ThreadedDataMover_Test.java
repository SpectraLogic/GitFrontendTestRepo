/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.io;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.commons.io.FileUtils;

import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.io.ThreadedDataMover.BytesReadListener;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.SystemWorkPool;
import org.junit.jupiter.api.TestInfo;

public final class ThreadedDataMover_Test 
{

    @Test
    public void testConstructorNullInputStreamProviderNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws IOException
            {
                new ThreadedDataMover(
                        DEFAULT_BUFFER_SIZE,
                        DEFAULT_BUFFER_SIZE,
                        getClass().getName(), 
                        1,
                        null,
                        new FileOutputStream( getOutputFile() ), 
                        null,
                        null );
            }
        } );
    }
    

    @Test
    public void testDataMovedSuccessfullyWhenNoListener() throws IOException
    {
        final File originalFile = getInputFile( 1 );
        final File outputFile = getOutputFile();
        originalFile.deleteOnExit();
        outputFile.deleteOnExit();
        final FileOutputStream fos = new FileOutputStream( outputFile );
        final FileInputStream fis = new FileInputStream( originalFile );
        new ThreadedDataMover(
                DEFAULT_BUFFER_SIZE,
                DEFAULT_BUFFER_SIZE,
                getClass().getName(), 
                1 * 1024,
                null,
                fos, 
                new SingleInputStreamProvider( fis ),
                null ).run();
        assertEquals(1024 * 1,  outputFile.length(), "Shoulda written input file to output file.");
        fos.close();
        fis.close();
        originalFile.delete();
        outputFile.delete();
    }
    

    @Test
    public void testRunWhenInputStreamByteLengthMismatchResultsInError() throws IOException
    {
        final File originalFile = getInputFile( 1 );
        final File outputFile = getOutputFile();
        originalFile.deleteOnExit();
        outputFile.deleteOnExit();
        final FileOutputStream fos = new FileOutputStream( outputFile );
        final FileInputStream fis = new FileInputStream( originalFile );

        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test()
            {
                new ThreadedDataMover(
                        DEFAULT_BUFFER_SIZE,
                        DEFAULT_BUFFER_SIZE,
                        getClass().getName(), 
                        1 * 1024 + 1,
                        null,
                        fos, 
                        new SingleInputStreamProvider( fis ),
                        null ).run();
            }
        } );
        assertEquals(1024 * 1,  outputFile.length(), "Shoulda written input file to output file.");
        fos.close();
        fis.close();
        originalFile.delete();
        outputFile.delete();
    }
    
    
    @Test
    public void testRunTwiceNotAllowed() throws IOException
    {
        final ChecksumType algorithm = ChecksumType.CRC_32;
        final int mb = 1;
        final CountingBytesReadListener listener = new CountingBytesReadListener();
        final File originalFile = getInputFile( mb * 1024 );
        originalFile.deleteOnExit();
        final FileInputStream fis = new FileInputStream( originalFile );
        final ThreadedDataMover mover = new ThreadedDataMover(
                DEFAULT_BUFFER_SIZE,
                DEFAULT_BUFFER_SIZE,
                null, 
                1024 * 1024 * mb,
                algorithm,
                null,
                new SingleInputStreamProvider( fis ),
                listener );
        mover.run();
        assertEquals(1024 * 1024 * mb,  listener.m_totalBytes, "Shoulda written input file to output file.");
        assertEquals(1024 * 1024 * mb,  mover.getTotalBytesMoved(), "Shoulda written input file to output file.");
        assertEquals(1024 * 1024 * mb,  mover.getTotalBytesMoved(), "Shoulda written input file to output file.");

        fis.close();
        if ( null != algorithm )
        {
            assertTrue(Arrays.equals( mover.getChecksum(), getCorrectChecksum( algorithm, originalFile ) ), "Shoulda calculated correct checksum for " + algorithm + ".");
        }
        originalFile.delete();
        
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                mover.run();
            }
        } );
    }
    
    
    @Test
    public void testNullOutputStreamResultsInCrcComputationOnly() throws IOException
    {
        final ChecksumType algorithm = ChecksumType.CRC_32;
        final int mb = 1;
        final CountingBytesReadListener listener = new CountingBytesReadListener();
        final File originalFile = getInputFile( mb * 1024 );
        originalFile.deleteOnExit();
        final FileInputStream fis = new FileInputStream( originalFile );
        final ThreadedDataMover mover = new ThreadedDataMover(
                DEFAULT_BUFFER_SIZE,
                DEFAULT_BUFFER_SIZE,
                getClass().getName(), 
                1024 * 1024 * mb,
                algorithm,
                null,
                new SingleInputStreamProvider( fis ),
                listener );
        mover.run();
        assertEquals(1024 * 1024 * mb,  listener.m_totalBytes, "Shoulda written input file to output file.");
        assertEquals(1024 * 1024 * mb,  mover.getTotalBytesMoved(), "Shoulda written input file to output file.");
        assertEquals(1024 * 1024 * mb,  mover.getTotalBytesMoved(), "Shoulda written input file to output file.");

        fis.close();
        if ( null != algorithm )
        {
            assertTrue(Arrays.equals( mover.getChecksum(), getCorrectChecksum( algorithm, originalFile ) ), "Shoulda calculated correct checksum for " + algorithm + ".");
        }
        originalFile.delete();
    }
    

    @Test
    public void testDataMovedSuccessfullyWhenVerySmallDataWithSimpleByteRange(TestInfo testInfo)
            throws IOException
    {
        final CountingBytesReadListener listener = new CountingBytesReadListener();
        final File originalFile = getInputFile( 64 );
        final File outputFile = getOutputFile();
        originalFile.deleteOnExit();
        outputFile.deleteOnExit();
        final FileOutputStream fos = new FileOutputStream( outputFile );
        final FileInputStream fis = new FileInputStream( originalFile );
        new ThreadedDataMover(
                DEFAULT_BUFFER_SIZE,
                DEFAULT_BUFFER_SIZE,
                testInfo.getDisplayName(),
                64 * 1024,
                new ByteRangesImpl( "bytes=10-11", 64 * 1024 ),
                null,
                fos,
                new SingleInputStreamProvider( fis ),
                listener ).run();
        assertEquals(2,  outputFile.length(), "Shoulda written input file to output file.");
        assertEquals(2,  listener.m_totalBytes, "Check how many bytes are read from the input file.");
        fos.close();
        fis.close();
        originalFile.delete();
        outputFile.delete();
    }

    @Test
    public void testDataMovedSuccessfullyWhenRangeIsAboveBufferSize(TestInfo testInfo)
            throws IOException
    {
        final CountingBytesReadListener listener = new CountingBytesReadListener();
        final File originalFile = getInputFile( 164 );
        final File outputFile = getOutputFile();
        originalFile.deleteOnExit();
        outputFile.deleteOnExit();
        final FileOutputStream fos = new FileOutputStream( outputFile );
        final FileInputStream fis = new FileInputStream( originalFile );
        new ThreadedDataMover(
                1024,
                1024,
                testInfo.getDisplayName(),
                64 * 1024,
                new ByteRangesImpl( "bytes=10-1600", 164 * 1024 ),
                null,
                fos,
                new SingleInputStreamProvider( fis ),
                listener ).run();
        assertEquals(1591,  outputFile.length(), "Shoulda written input file to output file.");
        assertEquals(1591,  listener.m_totalBytes, "Check how many bytes are read from the input file.");
        fos.close();
        fis.close();
        originalFile.delete();
        outputFile.delete();
    }

    @Test
    public void testDataMovedSuccessfullyWhenRangeSpansMultipleBuffer(TestInfo testInfo)
            throws IOException
    {
        final CountingBytesReadListener listener = new CountingBytesReadListener();
        final File originalFile = getInputFile( 164 );
        final File outputFile = getOutputFile();
        originalFile.deleteOnExit();
        outputFile.deleteOnExit();
        final FileOutputStream fos = new FileOutputStream( outputFile );
        final FileInputStream fis = new FileInputStream( originalFile );
        new ThreadedDataMover(
                1024,
                1024,
                testInfo.getDisplayName(),
                64 * 1024,
                new ByteRangesImpl( "bytes=10-5120", 164 * 1024 ),
                null,
                fos,
                new SingleInputStreamProvider( fis ),
                listener ).run();
        assertEquals(5111,  outputFile.length(), "Shoulda written input file to output file.");
        assertEquals(5111,  listener.m_totalBytes, "Check how many bytes are read from the input file.");
        fos.close();
        fis.close();
        originalFile.delete();
        outputFile.delete();
    }

    @Test
    public void testDataMovedSuccessfullyWhenVerySmallDataWithNoRange()
            throws IOException
    {
        final CountingBytesReadListener listener = new CountingBytesReadListener();
        final File originalFile = getInputFile( 64 );
        final File outputFile = getOutputFile();
        originalFile.deleteOnExit();
        outputFile.deleteOnExit();
        final FileOutputStream fos = new FileOutputStream( outputFile );
        final FileInputStream fis = new FileInputStream( originalFile );
        new ThreadedDataMover(
                1024,
                1024,
                getClass().getName(),
                64 * 1024,
                null,
                fos,
                new SingleInputStreamProvider( fis ),
                listener ).run();
        assertEquals(65536,  outputFile.length(), "Check bytes written to output file.");
        assertEquals(1024 * 64,  listener.m_totalBytes, "Checking bytes read from input file.");
        fos.close();
        fis.close();
        originalFile.delete();
        outputFile.delete();
    }

    @Test
    public void testDataMovedSuccessfullyWhenVerySmallDataWithComplexByteRange() 
            throws IOException
    {
        final CountingBytesReadListener listener = new CountingBytesReadListener();
        final File originalFile = getInputFile( 64 );
        final File outputFile = getOutputFile();
        originalFile.deleteOnExit();
        outputFile.deleteOnExit();
        final FileOutputStream fos = new FileOutputStream( outputFile );
        final FileInputStream fis = new FileInputStream( originalFile );
        new ThreadedDataMover(
                1024,
                1024,
                getClass().getName(),
                64 * 1024,
                new ByteRangesImpl( "bytes=0-1,10-11,1000-1099,5000-14999,64000-64001", 64 * 1024 ),
                null,
                fos,
                new SingleInputStreamProvider( fis ),
                listener ).run();
        assertEquals(10106,  outputFile.length(), "Shoulda written input file to output file.");
        assertEquals(10106,  listener.m_totalBytes, "Checking bytes read.");
        fos.close();
        fis.close();
        originalFile.delete();
        outputFile.delete();
    }

    @Test
    public void testDataMovedSuccessfullyWhenSplitRange()
            throws IOException
    {
        final CountingBytesReadListener listener = new CountingBytesReadListener();
        final File originalFile = getInputFile( 524288 );// 50G
        final File outputFile = getOutputFile();
        originalFile.deleteOnExit();
        outputFile.deleteOnExit();
        final FileOutputStream fos = new FileOutputStream( outputFile );
        final FileInputStream fis = new FileInputStream( originalFile );
        new ThreadedDataMover(
                1024,
                1024,
                getClass().getName(),
                524288 * 1024,
                new ByteRangesImpl( "bytes=65535997-65535998", 524288 * 1024 ),
                null,
                fos,
                new SingleInputStreamProvider( fis ),
                listener ).run();
        assertEquals(2,  outputFile.length(), "Shoulda written input file to output file.");
        assertEquals(2,  listener.m_totalBytes, "Checking bytes read.");
        fos.close();
        fis.close();
        originalFile.delete();
        outputFile.delete();
    }

    @Test
    public void testDataMovedSuccessfullyWhenBigDataWithComplexByteRange()
            throws IOException
    {
        final CountingBytesReadListener listener = new CountingBytesReadListener();
        final File originalFile = getInputFile( 80 );
        final File outputFile = getOutputFile();
        originalFile.deleteOnExit();
        outputFile.deleteOnExit();
        final FileOutputStream fos = new FileOutputStream( outputFile );
        final FileInputStream fis = new FileInputStream( originalFile );
        new ThreadedDataMover(
                1024,
                1024,
                getClass().getName(),
                8 * 1024,
                new ByteRangesImpl( "bytes=0-3000", 8 * 1024 ),
                null,
                fos,
                new SingleInputStreamProvider( fis ),
                listener ).run();
        assertEquals(3001,  outputFile.length(), "Should write input file to output file.");
        assertEquals(3001,  listener.m_totalBytes, "Checking bytes read.");
        fos.close();
        fis.close();
        originalFile.delete();
        outputFile.delete();
    }

    @Test
    public void testDataMovedSuccessfullyEdgeCases()
            throws IOException
    {
        final CountingBytesReadListener listener = new CountingBytesReadListener();
        final File originalFile = getInputFile( 80 );
        final File outputFile = getOutputFile();
        originalFile.deleteOnExit();
        outputFile.deleteOnExit();
        final FileOutputStream fos = new FileOutputStream( outputFile );
        final FileInputStream fis = new FileInputStream( originalFile );
        new ThreadedDataMover(
                1024,
                1024,
                getClass().getName(),
                8 * 1024,
                new ByteRangesImpl( "bytes=0-10,11-1021,4085-4094,4095-5119", 80 * 1024 ),
                null,
                fos,
                new SingleInputStreamProvider( fis ),
                listener ).run();
        assertEquals(2057,  outputFile.length(), "Should write input file to output file.");
        assertEquals(2057,  listener.m_totalBytes, "Checking bytes read.");
        fos.close();
        fis.close();
        originalFile.delete();
        outputFile.delete();
    }

    @Test
    public void testDataMovedSuccessfullyProblemRange()
            throws IOException
    {
        final CountingBytesReadListener listener = new CountingBytesReadListener();
        final File originalFile = getInputFile( 80 );
        final File outputFile = getOutputFile();
        originalFile.deleteOnExit();
        outputFile.deleteOnExit();
        final FileOutputStream fos = new FileOutputStream( outputFile );
        final FileInputStream fis = new FileInputStream( originalFile );
        new ThreadedDataMover(
                1024,
                1024,
                getClass().getName(),
                8 * 1024,
                new ByteRangesImpl( "bytes=0-1026,4095-7148", 80 * 1024 ),
                null,
                fos,
                new SingleInputStreamProvider( fis ),
                listener ).run();
        assertEquals(4081,  outputFile.length(), "Should write input file to output file.");
        assertEquals(4081,  listener.m_totalBytes, "Checking bytes read.");
        fos.close();
        fis.close();
        originalFile.delete();
        outputFile.delete();
    }

    @Test
    public void testDataMovedSuccessfullyWhenVaryingByteRanges() throws IOException
    {
        for ( final int firstOffset : new int [] { 0, 1022, 1023, 1024, 1025 } )
        {
            for ( final int firstLength : new int [] { 1022, 1023, 1024, 1025, 2023, 2024, 2025 } )
            {
                for ( final int secondOffset : new int [] { 4095, 4096, 4097 } )
                {
                    for ( final int secondLength : new int [] { 0, 1023, 1024, 1025, 2023, 2024, 2025 } )
                    {
                        internalTestDataMovedSuccessfullyWhenVaryingByteRanges(
                                firstOffset, firstLength, secondOffset, secondLength );
                    }
                }
            }
        }
    }
    
    
    private void internalTestDataMovedSuccessfullyWhenVaryingByteRanges( 
            final int firstOffset, 
            final int firstLength,
            final int secondOffset, 
            final int secondLength ) throws IOException
    {
        String byteRangesString = "bytes=" + firstOffset + "-" + ( firstOffset + firstLength - 1 );
        if ( 0 < secondLength )
        {
            byteRangesString += ", " + secondOffset + "-" + ( secondOffset + secondLength - 1 );
        }
        
        final CountingBytesReadListener listener = new CountingBytesReadListener();
        final File originalFile = getInputFile( 8 );
        final File outputFile = getOutputFile();
        originalFile.deleteOnExit();
        outputFile.deleteOnExit();
        final FileOutputStream fos = new FileOutputStream( outputFile );
        final FileInputStream fis = new FileInputStream( originalFile );
        new ThreadedDataMover(
                1024,
                1024,
                getClass().getName(), 
                8 * 1024,
                new ByteRangesImpl( byteRangesString, 8 * 1024 ),
                null,
                fos,
                new SingleInputStreamProvider( fis ),
                listener ).run();
        fis.close();

        assertEquals(firstLength + secondLength, outputFile.length(), "Shoulda written input file to output file for " + byteRangesString + ".");
        assertEquals(firstLength + secondLength,  listener.m_totalBytes, "Check number of bytes read " + byteRangesString + ".");

        final byte [] originalBytes = new byte[ 8 * 1024 ];
        final byte [] newBytes = new byte[ firstLength + secondLength ];
        final FileInputStream ois = new FileInputStream( originalFile );
        ois.read( originalBytes, 0, originalBytes.length );
        ois.close();
        final FileInputStream nis = new FileInputStream( outputFile );
        nis.read( newBytes, 0, newBytes.length );
        nis.close();
        
        int newOffset = -1;
        for ( int i = firstOffset; i < firstOffset + firstLength; ++i )
        {
            assertEquals(originalBytes[ i ],
                    newBytes[ ++newOffset ],
                    "Shoulda copied correct bytes for " + byteRangesString
                        + " (was incorrect at position " + i + ").");
        }
        for ( int i = secondOffset; i < secondOffset + secondLength; ++i )
        {
            assertEquals(originalBytes[ i ],
                    newBytes[ ++newOffset ],
                    "Shoulda copied correct bytes for " + byteRangesString
                        + " (was incorrect at position " + i + ").");
        }
        fos.close();
        fis.close();
        originalFile.delete();
        outputFile.delete();
    }
    

    @Test
    public void testDataMovedSuccessfullyWhenVerySmallData() throws IOException
    {
        final CountingBytesReadListener listener = new CountingBytesReadListener();
        final File originalFile = getInputFile( 64 );
        final File outputFile = getOutputFile();
        originalFile.deleteOnExit();
        outputFile.deleteOnExit();
        final FileOutputStream fos = new FileOutputStream( outputFile );
        final FileInputStream fis = new FileInputStream( originalFile );
        new ThreadedDataMover(
                DEFAULT_BUFFER_SIZE,
                DEFAULT_BUFFER_SIZE,
                getClass().getName(), 
                64 * 1024,
                null,
                fos,
                new SingleInputStreamProvider( fis ),
                listener ).run();
        assertEquals(1024 * 64,  outputFile.length(), "Shoulda written input file to output file.");
        assertEquals(1024 * 64,  listener.m_totalBytes, "Shoulda written input file to output file.");
        fos.close();
        fis.close();
        originalFile.delete();
        outputFile.delete();
    }
    

    @Test
    public void testGetChecksumWhenChecksumNeverCalculatedNotAllowed() throws IOException
    {
        final int mb = 1;
        final CountingBytesReadListener listener = new CountingBytesReadListener();
        final File originalFile = getInputFile( mb * 1024 );
        final File outputFile = getOutputFile();
        originalFile.deleteOnExit();
        outputFile.deleteOnExit();
        final FileOutputStream fos = new FileOutputStream( outputFile );
        final FileInputStream fis = new FileInputStream( originalFile );
        final ThreadedDataMover mover = new ThreadedDataMover(
                DEFAULT_BUFFER_SIZE,
                DEFAULT_BUFFER_SIZE,
                getClass().getName(), 
                1024 * 1024 * mb,
                null,
                fos,
                new SingleInputStreamProvider( fis ),
                listener );
        mover.run();
        assertTrue(FileUtils.contentEquals( outputFile, originalFile ), "Shoulda written input file to output file.");
        assertEquals(1024 * 1024 * mb,  outputFile.length(), "Shoulda written input file to output file.");
        assertEquals(1024 * 1024 * mb,  listener.m_totalBytes, "Shoulda written input file to output file.");
        assertEquals(1024 * 1024 * mb,  mover.getTotalBytesMoved(), "Shoulda written input file to output file.");
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                mover.getChecksum();
            }
        } );
        fos.close();
        fis.close();
        originalFile.delete();
        outputFile.delete();
    }
    

    @Test
    public void testDataMovedSuccessfullyNoMatterTheBufferSizes() throws IOException
    {
        for ( int readBufferSize = 1024; readBufferSize <= 1024 * 1024; readBufferSize *= 4 )
        {
            for ( int writeBufferSize = 1024; writeBufferSize <= 1024 * 1024; writeBufferSize *= 4 )
            {
                final int kb = Math.min( 400, writeBufferSize * 36 );
                final CountingBytesReadListener listener = new CountingBytesReadListener();
                final File originalFile = getInputFile( kb );
                final File outputFile = getOutputFile();
                originalFile.deleteOnExit();
                outputFile.deleteOnExit();
                final FileOutputStream fos = new FileOutputStream( outputFile );
                final FileInputStream fis = new FileInputStream( originalFile );
                final ThreadedDataMover mover = new ThreadedDataMover(
                        readBufferSize,
                        writeBufferSize,
                        getClass().getName(), 
                        1024 * kb,
                        ChecksumType.CRC_32,
                        fos,
                        new SingleInputStreamProvider( fis ),
                        listener );
                mover.run();
                assertTrue(FileUtils.contentEquals( outputFile, originalFile ), "Shoulda written input file to output file.");
                assertEquals(1024 * kb, outputFile.length(), "Shoulda written input file to output file.");
                assertEquals(1024 * kb,  listener.m_totalBytes, "Shoulda written input file to output file.");
                assertEquals(1024 * kb, mover.getTotalBytesMoved(), "Shoulda written input file to output file.");
                assertTrue(Arrays.equals(
                                        mover.getChecksum(),
                                        getCorrectChecksum( ChecksumType.CRC_32, outputFile ) ), "Shoulda calculated correct checksum for CRC.");
                fos.close();
                fis.close();
                originalFile.delete();
                outputFile.delete();
            }
        }
    }
    

    @Test
    public void testDataMovedSuccessfullyWhenLargeDataNoMatterTheChecksumAlgorithm() throws IOException
    {
        internalTestDataMovedSuccessfullyWhenLargeData( null );
        for ( final ChecksumType algorithm : ChecksumType.values() )
        {
            internalTestDataMovedSuccessfullyWhenLargeData( algorithm );
        }
    }
    

    private void internalTestDataMovedSuccessfullyWhenLargeData( final ChecksumType algorithm ) 
            throws IOException
    {
        final int mb = 3;
        final CountingBytesReadListener listener = new CountingBytesReadListener();
        final File originalFile = getInputFile( mb * 1024 );
        final File outputFile = getOutputFile();
        originalFile.deleteOnExit();
        outputFile.deleteOnExit();
        final FileOutputStream fos = new FileOutputStream( outputFile );
        final FileInputStream fis = new FileInputStream( originalFile );
        final ThreadedDataMover mover = new ThreadedDataMover(
                DEFAULT_BUFFER_SIZE,
                DEFAULT_BUFFER_SIZE,
                ( ChecksumType.values()[ 2 ] == algorithm ) ? null : getClass().getName(), 
                1024 * 1024 * mb,
                algorithm,
                fos,
                new SingleInputStreamProvider( fis ),
                listener );
        mover.run();
        assertTrue(FileUtils.contentEquals( outputFile, originalFile ), "Shoulda written input file to output file.");
        assertEquals(1024 * 1024 * mb,  outputFile.length(), "Shoulda written input file to output file.");
        assertEquals(1024 * 1024 * mb,  listener.m_totalBytes, "Shoulda written input file to output file.");
        assertEquals(1024 * 1024 * mb,  mover.getTotalBytesMoved(), "Shoulda written input file to output file.");
        assertEquals(1024 * 1024 * mb,  mover.getTotalBytesMoved(), "Shoulda written input file to output file.");
        if ( null != algorithm )
        {
            assertTrue(Arrays.equals( mover.getChecksum(), getCorrectChecksum( algorithm, outputFile ) ), "Shoulda calculated correct checksum for " + algorithm + ".");
        }
        fos.close();
        fis.close();
        originalFile.delete();
        outputFile.delete();
    }
    

    @Test
    public void testConcurrentDataMoves() throws IOException, InterruptedException, ExecutionException
    {
        final int kb = 555;
        
        final int numberOfMovers = Runtime.getRuntime().availableProcessors() * 4;
        final ThreadedDataMover [] movers = new ThreadedDataMover[ numberOfMovers ];
        final File [] outputFiles = new File[ numberOfMovers ];
        final File [] inputFiles = new File[ numberOfMovers ];
        final Set< Closeable > streams = new HashSet<>();
        final Set< Future< ? > > futures = new HashSet<>();
        for ( int i = 0; i < movers.length; ++i )
        {
            outputFiles[ i ] = getOutputFile( "out" + String.valueOf( i ) );
            inputFiles[ i ] = getInputFile( "in" + String.valueOf( i ), kb );
            final FileOutputStream fos = new FileOutputStream( outputFiles[ i ] );
            final FileInputStream fis = new FileInputStream( inputFiles[ i ] );
            streams.add( fos );
            streams.add( fis );
            movers[ i ] = new ThreadedDataMover(
                    DEFAULT_BUFFER_SIZE,
                    DEFAULT_BUFFER_SIZE,
                    getClass().getName(), 
                    1024 * kb,
                    ChecksumType.MD5,
                    fos, 
                    new SingleInputStreamProvider( fis ),
                    null );
        }
        for ( int i = 0; i < movers.length; ++i )
        {
            futures.add( SystemWorkPool.getInstance().submit( movers[ i ] ) );
        }
        for ( final Future< ? > f : futures )
        {
            f.get();
        }
        for ( final Closeable stream : streams )
        {
            stream.close();
        }
        for ( int i = 0; i < movers.length; ++i )
        {
            assertTrue(FileUtils.contentEquals( outputFiles[ i ], inputFiles[ i ] ), "Shoulda written input file to output file.");
            inputFiles[ i ].delete();
            outputFiles[ i ].delete();
        }
    }
    

    @Test
    public void testConcurrentDataMovesWithMultipleInputStreamBackers()
            throws IOException, InterruptedException, ExecutionException
    {
        final int kb = 555;
        
        final int numberOfInputStreamsPerMover = 3;
        final int numberOfMovers = Runtime.getRuntime().availableProcessors() * 4;
        final ThreadedDataMover [] movers = new ThreadedDataMover[ numberOfMovers ];
        final File [] outputFiles = new File[ numberOfMovers ];
        final List< List< File > > inputFiles = new ArrayList<>();
        final Set< Closeable > streams = new HashSet<>();
        final Set< Future< ? > > futures = new HashSet<>();
        for ( int i = 0; i < movers.length; ++i )
        {
            outputFiles[ i ] = getOutputFile( "out" + String.valueOf( i ) );
            final List< File > myFiles = new ArrayList<>();
            for ( int j = 0; j < numberOfInputStreamsPerMover; ++j )
            {
                myFiles.add( getInputFile( "in" + String.valueOf( i ) + "-" + String.valueOf( j ), kb ) );
            }
            inputFiles.add( myFiles );
            
            final FileOutputStream fos = new FileOutputStream( outputFiles[ i ] );
            streams.add( fos );
            movers[ i ] = new ThreadedDataMover(
                    DEFAULT_BUFFER_SIZE,
                    DEFAULT_BUFFER_SIZE,
                    getClass().getName(), 
                    1024 * kb * numberOfInputStreamsPerMover,
                    ChecksumType.MD5,
                    fos, 
                    new FileDeletingInputStreamProvider( myFiles ),
                    null );
        }
        for ( int i = 0; i < movers.length; ++i )
        {
            futures.add( SystemWorkPool.getInstance().submit( movers[ i ] ) );
        }
        for ( final Future< ? > f : futures )
        {
            f.get();
        }
        for ( final Closeable stream : streams )
        {
            stream.close();
        }
        for ( int i = 0; i < movers.length; ++i )
        {
            for ( final File f : inputFiles.get( i ) )
            {
                assertFalse(
                        f.exists(),
                        "Shoulda deleted input file."
                        );
            }
            assertEquals(kb * numberOfInputStreamsPerMover * 1024,  outputFiles[i].length(), "Shoulda created output file.");
            outputFiles[ i ].delete();
        }
    }
    
    
    private final static class CountingBytesReadListener implements BytesReadListener
    {
        public void bytesRead( int numberOfBytes )
        {
            m_totalBytes += numberOfBytes;
        }
        
        private volatile int m_totalBytes;
    } // end inner class def
    
    
    private File getOutputFile()
    {
        return getOutputFile( "out" );
    }
    
    
    private File getOutputFile( final String fileName )
    {
        try
        {
            final File retval = File.createTempFile( ThreadedDataMover.class.getSimpleName(), fileName );
            retval.deleteOnExit();
            return retval;
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    private File getInputFile( final int kbOfData )
    {
        return getInputFile( "in", kbOfData );
    }
    
    
    private File getInputFile( final String fileName, final int kbOfData )
    {
        try
        {
            final File retval = File.createTempFile( ThreadedDataMover.class.getSimpleName(), fileName );
            retval.deleteOnExit();
            
            final int length = 1024 * kbOfData;
            final byte [] data = new byte[ length ];
            for ( int i = 0; i < data.length; ++i )
            {
                data[ i ] = (byte)( i % 100 );
            }
            final FileOutputStream out = new FileOutputStream( retval );
            out.write( data, 0, length );
            out.close();
            
            return retval;
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    private byte [] getCorrectChecksum( final ChecksumType algorithm, final File outputFile )
    {
        switch ( algorithm )
        {
            case MD5:
                return getMd5( outputFile );
            case CRC_32:
                return getCrc32( outputFile );
            case CRC_32C:
                return getCrc32c( outputFile );
            case SHA_256:
                return getSha256( outputFile );
            case SHA_512:
                return getSha512( outputFile );
            default:
                throw new UnsupportedOperationException( "Not supported: " + algorithm );
        }
    }
    
    
    private byte [] getMd5( final File file )
    {
        try
        {
            final FileInputStream fis = new FileInputStream( file );
            final byte [] md5 = org.apache.commons.codec.digest.DigestUtils.md5( fis );
            fis.close();
            return md5;
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    private byte [] getSha256( final File file )
    {
        try
        {
            final FileInputStream fis = new FileInputStream( file );
            final byte [] md5 = org.apache.commons.codec.digest.DigestUtils.sha256( fis );
            fis.close();
            return md5;
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    private byte [] getSha512( final File file )
    {
        try
        {
            final FileInputStream fis = new FileInputStream( file );
            final byte [] md5 = org.apache.commons.codec.digest.DigestUtils.sha512( fis );
            fis.close();
            return md5;
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    private byte [] getCrc32( final File file )
    {
        try
        {
            int length = 0;
            final byte [] buffer = new byte[ 1024 * 1024 ];
            final CRC32 crc = new CRC32();
            final FileInputStream fis = new FileInputStream( file );
            while ( 0 <= length )
            {
                length = fis.read( buffer );
                if ( 0 < length )
                {
                    crc.update( buffer, 0, length );
                }
            }
            fis.close();
            
            final long checksum = crc.getValue();
            final byte [] retval = new byte[ 4 ];  
            for ( int i = 0; i < 4; ++i )
            {  
               retval[ 3 - i ] = (byte)( checksum >>> ( i * 8 ) );  
            }  
            return retval;
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    private byte [] getCrc32c( final File file )
    {
        try
        {
            final Constructor< ? > con = Class.forName( "io.netty.handler.codec.compression.Crc32c" )
                    .getDeclaredConstructor();
            con.setAccessible( true );
            int length = 0;
            final byte [] buffer = new byte[ 1024 * 1024 ];
            final Checksum crc = (Checksum)con.newInstance();
            final FileInputStream fis = new FileInputStream( file );
            while ( 0 <= length )
            {
                length = fis.read( buffer );
                if ( 0 < length )
                {
                    crc.update( buffer, 0, length );
                }
            }
            fis.close();
            
            final long checksum = crc.getValue();
            final byte [] retval = new byte[ 4 ];  
            for ( int i = 0; i < 4; ++i )
            {  
               retval[ 3 - i ] = (byte)( checksum >>> ( i * 8 ) );  
            }  
            return retval;
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    private final static int DEFAULT_BUFFER_SIZE = 1024 * 8;
}
