/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.io;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.io.lang.ByteRanges;
import com.spectralogic.util.io.lang.InputStreamProvider;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.math.LongRange;
import com.spectralogic.util.lang.math.LongRangeImpl;
import com.spectralogic.util.render.BytesRenderer;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.security.FastMD5;
import com.spectralogic.util.thread.workmon.MonitoredWork;
import com.spectralogic.util.thread.workmon.MonitoredWork.StackTraceLogging;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;

/**
 * Moves data from an input stream to an output stream while computing a checksum hash.
 */
public final class ThreadedDataMover implements Runnable
{
    public ThreadedDataMover( 
            final int readBufferSizeInBytes,
            final int writeBufferSizeInBytes,
            final String dataTransferName,
            final long expectedTotalBytes,
            final ChecksumType checksumAlgorithm,
            final OutputStream out,
            final InputStreamProvider inputProvider,
            final BytesReadListener bytesReadListener )
    {
        this( readBufferSizeInBytes, 
              writeBufferSizeInBytes, 
              dataTransferName, 
              expectedTotalBytes, 
              null,
              checksumAlgorithm,
              out, 
              inputProvider, 
              bytesReadListener );
    }

    public ThreadedDataMover( 
            final int readBufferSizeInBytes,
            final int writeBufferSizeInBytes,
            final String dataTransferName,
            final long expectedTotalBytes,
            final ByteRanges byteRangesFromInputStreamToForwardToOutputStream,
            final ChecksumType checksumAlgorithm,
            final OutputStream out,
            final InputStreamProvider inputProvider,
            final BytesReadListener bytesReadListener )
    {
        initWorkPools();
        
        Validations.verifyInRange( "Read buffer size", 
                1024, MAX_BUFFER_CAPACITY / 6, readBufferSizeInBytes );
        Validations.verifyInRange( "Write buffer size", 
                1024, MAX_BUFFER_CAPACITY / 6, writeBufferSizeInBytes );
        Validations.verifyNotNull( "Input provider", inputProvider );
        if ( null != byteRangesFromInputStreamToForwardToOutputStream )
        {
            Validations.verifyInRange( 
                    "Offset",
                    0, 
                    expectedTotalBytes - 1, 
                    byteRangesFromInputStreamToForwardToOutputStream
                        .getFullRequiredRange().getStart() );
            Validations.verifyInRange( 
                    "Length",
                    0, 
                    expectedTotalBytes - 1, 
                    byteRangesFromInputStreamToForwardToOutputStream
                        .getFullRequiredRange().getEnd() );
        }
        
        m_readBuffer = new byte[ readBufferSizeInBytes ];
        m_writeBufferSize = writeBufferSizeInBytes;
        m_numberOfWriteBuffers = Math.min( MAX_BUFFER_CAPACITY / m_writeBufferSize, MAX_NUMBER_OF_BUFFERS );
        m_bytesReadListener = bytesReadListener;
        m_dataTransferName = dataTransferName;
        m_byteRanges = byteRangesFromInputStreamToForwardToOutputStream;
        m_os = out;
        m_inputProvider = inputProvider;
        m_checksumAlgorithm = checksumAlgorithm;
        m_expectedTotalBytes = (m_byteRanges == null) ? expectedTotalBytes : m_byteRanges.getAggregateLength();
    }
    
    
    public void run()
    {
        if ( m_runCalled.getAndSet( true ) )
        {
            throw new IllegalStateException( "Run already called." );
        }

        if ( null != m_dataTransferName )
        {
            LOG.info( m_dataTransferName + " starting..." );
            if ( null == m_byteRanges )
            {
                LOG.info( m_dataTransferName + " requires " + BYTES_RENDERER.render( m_expectedTotalBytes ) 
                        + " to be transferred." );
            }
            else
            {
                LOG.info( m_dataTransferName + " requires " + BYTES_RENDERER.render( m_expectedTotalBytes ) 
                        + " to be read, but only " + m_byteRanges
                        + " to be written.  " + getClass().getSimpleName() 
                        + " assumes underlying streams are non-seekable, " 
                        + "so performance may be sub-optimal." );
            }
        }
        
        final Duration duration = new Duration();
        try
        {
            moveData();
        }
        finally
        {
            try
            {
                if ( null != m_os )
                {
                    m_os.flush();
                }
            }
            catch ( final IOException ex )
            {
                LOG.warn( "Failed to close output stream.", ex );
            }
        }
        
        if ( null != m_dataTransferName )
        {
            if ( 5 > duration.getElapsedMillis() )
            {
                LOG.info( m_dataTransferName 
                          + " completed (" + BYTES_RENDERER.render( m_totalBytesRead ) + " transferred)." );
            }
            else
            {
                LOG.info( m_dataTransferName + " completed (" + BYTES_RENDERER.render( m_totalBytesRead ) +
                        " transferred at " 
                        + BYTES_RENDERER.render( m_totalBytesRead, duration ) + ")." );
            }
            if ( null == m_checksum )
            {
                LOG.info( "No cryptographic checksum was computed for the data transferred." );
            }
            else
            {
                LOG.info( m_checksumAlgorithm + " checksum for data transferred is "
                          + Hex.encodeHexString( m_checksum ) + "." );
            }
        }
        
        if ( m_expectedTotalBytes != m_totalBytesRead )
        {
            throw new FailureTypeObservableException( 
                    GenericFailure.BAD_REQUEST,
                    "Expected " + m_expectedTotalBytes + " bytes, but there were " + m_totalBytesRead 
                    + " bytes." );
        }
    }

    private ChecksumComputer getChecksumComputer() {
        if (null == m_checksumAlgorithm) {
            return null;
        }
        switch (m_checksumAlgorithm) {
            case CRC_32:
                return new Crc32ChecksumComputer(CRC32.class.getName());
            case MD5:
                return new FastMD5ChecksumComputer();
            case CRC_32C:
                return new Crc32ChecksumComputer("io.netty.handler.codec.compression.Crc32c");
            default:
                return new CryptographicChecksumComputer(m_checksumAlgorithm.getAlgorithmName());
        }
    }


    private void moveData() {
        final DataWriter writer = new DataWriter( m_numberOfWriteBuffers, m_os, m_byteRanges );

        final ChecksumComputer checksumComputer = getChecksumComputer();

        /*
         * Since there are fewer checksum threads than data transfer threads, acquire a checksum thread before
         * acquiring a data transfer thread.  To ensure we don't acquire a data transfer thread while we're
         * blocked waiting for a checksum thread, use a ready latch to prevent the data transfer thread from
         * being acquired until we're really ready to acquire it.
         *
         * To ensure that we don't proceed to allocate buffer space and note that acquisition of threads has
         * completed prematurely, wait until both the checksum and data transfer threads have been acquired
         * and are ready before we proceed.
         */
        final Future< ? > futureWriter;
        final Future< ? > futureChecksumComputer;
        final ChecksumBufferProcessor checksumBufferProcessor;
        final MonitoredWork work = new MonitoredWork(
                StackTraceLogging.NONE, "Acquire threads for data move" );
        try
        {
            checksumBufferProcessor = ( null == checksumComputer ) ?
                    null
                    : new ChecksumBufferProcessor( m_numberOfWriteBuffers, checksumComputer );
            futureChecksumComputer = ( null == checksumComputer ) ?
                    null
                    : s_checksumWP.submit( checksumBufferProcessor );
            if ( null != checksumBufferProcessor )
            {
                checksumBufferProcessor.m_readyLatch.await();
            }
            futureWriter = s_writeDataWP.submit( writer );
            writer.m_readyLatch.await();
        }
        catch ( final InterruptedException ex )
        {
            throw new RuntimeException( ex );
        }
        finally
        {
            work.completed();
        }

        final List< Buffer > buffers = new ArrayList<>();
        //NOTE: The reason we add +2 to the number of buffers is to prevent concurrently reading from and writing to
        //the same buffer. The reader does not explicitly free the buffer when it is done, the only thing that is
        //preventing overlap is the blocking behavior of the queue. For example, if we have 10 buffers available and
        //a queue size of 8, we might have buffer #1 being read from, buffer #2-#9 in the queue, and buffer #10 being
        //written to. Once the writing is complete, we will try to add it to the queue and block until the reader takes
        //buffer #2 out (which is how we know it is done with #1). Essentially you "free" buffer N by taking buffer N+1
        for ( int i = 0; i < m_numberOfWriteBuffers + 2; ++i )
        {
            final Buffer buffer = new Buffer( new byte[ m_writeBufferSize ] );
            buffers.add( buffer );
        }

        long sizeSinceLastTime = 0;
        Duration durationSinceLoggedProgress = new Duration();
        int bufferIndex = 0;
        InputStream in = null;
        try {
            if ( null != writer.m_writeFailure )
            {
                throw writer.m_writeFailure;
            }
            in = m_inputProvider.getNextInputStream();
            if (m_byteRanges != null && !m_byteRanges.getRanges().isEmpty()) {
                rangedCopy(in, buffers, writer, checksumBufferProcessor, durationSinceLoggedProgress, sizeSinceLastTime);
            } else {
                while ( true )
                {
                    final Buffer buffer = buffers.get( bufferIndex );
                    if (null == in && (in = m_inputProvider.getNextInputStream()) == null) {
                        break;
                    }
                    int offset = 0;

                    while ( offset < m_writeBufferSize )
                    {
                        final int bytesRead = read( in, buffer.m_buffer, offset, m_writeBufferSize - offset );
                        if ( 0 > bytesRead )
                        {
                            in = m_inputProvider.getNextInputStream();
                            if ( null == in )
                            {
                                if ( 0 == offset )
                                {
                                    offset = -1;
                                }
                                break;
                            }
                        }
                        else
                        {
                            offset += bytesRead;
                        }
                    }
                    buffer.m_length = offset;
                    if ( 0 > offset )
                    {
                        break;
                    }

                    bufferIndex = processBuffer(bufferIndex, writer, checksumBufferProcessor, buffers);

                    if (0 < durationSinceLoggedProgress.getElapsedMinutes()) {
                        LOG.info(m_dataTransferName + " " + (m_totalBytesRead * 100 / m_expectedTotalBytes)
                                + "% complete (" + BYTES_RENDERER.render(m_totalBytesRead)
                                + " transferred so far at "
                                + BYTES_RENDERER.render(
                                m_totalBytesRead - sizeSinceLastTime, durationSinceLoggedProgress)
                                + ").");
                        durationSinceLoggedProgress = new Duration();
                        sizeSinceLastTime = m_totalBytesRead;
                    }


                }
            }
        } catch ( final Exception ex )
        {
            sendFinishedSignal( writer, checksumBufferProcessor );
            handleException( ex );
        }

        try
        {
            sendFinishedSignal( writer, checksumBufferProcessor );
            futureWriter.get();
            if ( null != futureChecksumComputer )
            {
                futureChecksumComputer.get();
            }
            if ( null != writer.m_writeFailure )
            {
                throw writer.m_writeFailure;
            }
            m_checksum = ( null == checksumComputer ) ?
                    null
                    : checksumComputer.getChecksum();
        }
        catch ( final Exception ex )
        {
            handleException( ex );
        }
    }


    private int read( final InputStream in, final byte [] buffer, final int offset, final int maxLength )
        throws IOException
    {
        if ( m_readBufferOffset == m_readBufferLength )
        {
            m_readBufferOffset = 0;
            m_readBufferLength = in.read( m_readBuffer, 0, m_readBuffer.length );
            if ( 0 > m_readBufferLength )
            {
                m_readBufferOffset = 0;
                m_readBufferLength = 0;
                return -1;
            }
        }
        
        final int length = Math.min( maxLength, m_readBufferLength - m_readBufferOffset );
        System.arraycopy( m_readBuffer, m_readBufferOffset, buffer, offset, length );
        m_readBufferOffset += length;
        return length;
    }


    private void rangedCopy(InputStream in, List< Buffer > buffers, DataWriter writer,ChecksumBufferProcessor checksumBufferProcessor,Duration durationSinceLoggedProgress, long sizeSinceLastTime) throws InterruptedException, IOException {
        int bufferIndex = 0;
        Buffer buffer = buffers.get( bufferIndex );
        int index = 0;
        long totalBytes = 0;
        LongRange prev = new LongRangeImpl( -1, -1 );
        long skipValue = 0;
        int emptyBufferSize = m_writeBufferSize;
        for (LongRange range : m_byteRanges.getRanges()) {
            if (prev.getStart() != -1) {
                skipValue = range.getStart() - prev.getEnd() - 1;
            } else {
                skipValue = range.getStart();
            }

            try{
                // Skip to the start position
                long skipped = in.skip(skipValue);
                while (skipped < skipValue) {
                    long currentSkip = in.skip(skipValue - skipped);
                    if (currentSkip == 0)  {
                        throw new IOException("Failed to skip to the start position");
                    }
                    skipped += currentSkip;
                }

                long offset = 0;
                while (offset < range.getLength() ) {
                    long maxlength = range.getLength() - offset;
                    maxlength = Math.min(maxlength, emptyBufferSize);

                    final int bytesRead = in.read(buffer.m_buffer, index, (int)maxlength);
                    if (bytesRead <0) {
                        break;
                    } else if ( totalBytes+bytesRead >= m_writeBufferSize) {
                        buffer.m_length = (int) totalBytes+bytesRead;
                        bufferIndex = processBuffer(bufferIndex, writer, checksumBufferProcessor, buffers);
                        buffer = buffers.get( bufferIndex );
                        offset +=  bytesRead;
                        totalBytes = 0;
                        index = 0;
                        emptyBufferSize = m_writeBufferSize;
                    } else {
                        offset += bytesRead;
                        index += bytesRead;
                        totalBytes += bytesRead;
                        emptyBufferSize = m_writeBufferSize - (int) totalBytes;
                    }

                }
                if (0 < durationSinceLoggedProgress.getElapsedMinutes()) {
                    LOG.info(m_dataTransferName + " " + (m_totalBytesRead * 100 / m_expectedTotalBytes)
                            + "% complete (" + BYTES_RENDERER.render(m_totalBytesRead)
                            + " transferred so far at "
                            + BYTES_RENDERER.render(
                            m_totalBytesRead - sizeSinceLastTime, durationSinceLoggedProgress)
                            + ").");
                    durationSinceLoggedProgress = new Duration();
                    sizeSinceLastTime = m_totalBytesRead;
                }

            }catch (IOException | InterruptedException ex) {
                throw new RuntimeException(ex);
            }
            prev = range;

        }
        if (totalBytes  > 0) {
            buffer.m_length = (int) totalBytes;
            processBuffer(bufferIndex, writer, checksumBufferProcessor, buffers);

        }
    }

    private int processBuffer(int bufferIndex, DataWriter writer, ChecksumBufferProcessor checksumBufferProcessor, List<Buffer> buffers) throws InterruptedException, IOException {
        Buffer buffer = buffers.get( bufferIndex );
        m_totalBytesRead += buffer.m_length;
        if (null != m_bytesReadListener) {
            m_bytesReadListener.bytesRead(buffer.m_length);
        }
        writer.m_buffers.put(buffer);
        if (null != checksumBufferProcessor) {
            checksumBufferProcessor.m_buffers.put(buffer);
        }

        bufferIndex += 1;
        bufferIndex = bufferIndex % buffers.size();
        if ( null != writer.m_writeFailure )
        {
            throw writer.m_writeFailure;
        }
        return bufferIndex;
    }

    private void sendFinishedSignal(
            final DataWriter writer,
            final ChecksumBufferProcessor checksumBufferProcessor )
    {
        try
        {
            writer.m_buffers.put( DONE_TOKEN );
            if ( null != checksumBufferProcessor )
            {
                checksumBufferProcessor.m_buffers.put( DONE_TOKEN );
            }
        }
        catch ( final InterruptedException ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    private void handleException( final Exception ex )
    {
        if ( IOException.class.isAssignableFrom( ex.getClass() ) )
        {
            throw new FailureTypeObservableException( 
                    GenericFailure.RETRY_WITH_SYNCHRONOUS_WAIT,
                    "Failed to move data.  This might be due to a network hiccup and a retry might succeed.",
                    ex );
        }
        throw new RuntimeException( "Failed to move data.", ex );
    }
    
    
    private final static class DataWriter implements Runnable
    {
        private DataWriter( final int numberOfBuffers, final OutputStream out, final ByteRanges byteRanges )
        {
            m_out = out;
            m_buffers = new ArrayBlockingQueue<>( numberOfBuffers );
            m_byteRanges = ( null == byteRanges ) ? null : byteRanges.getRanges();
            takeNextByteRange( true );
        }
        
        
        private void takeNextByteRange( final boolean init )
        {
            if ( null == m_byteRanges )
            {
                return;
            }
            
            if ( m_byteRanges.isEmpty() || ( !init && 2 > m_byteRanges.size() ) )
            {
                m_length = -1;
                m_offset = -1;
                return;
            }
            
            final LongRange lastRange = ( init ) ? new LongRangeImpl( -1, -1 ) : m_byteRanges.remove( 0 );
            final LongRange nextRange = m_byteRanges.get( 0 );
            m_length = nextRange.getLength();
            m_offset = nextRange.getStart() - lastRange.getEnd() - 1;
        }
        
        
        public void run()
        {
            m_readyLatch.countDown();
            while ( true )
            {
                final Buffer buffer;
                try
                {
                    buffer = m_buffers.take();
                }
                catch ( final InterruptedException ex )
                {
                    throw new RuntimeException( ex );
                }
                
                if ( DONE_TOKEN == buffer )
                {
                    try
                    {
                        if ( null != m_out )
                        {
                            m_out.flush();
                        }
                    }
                    catch ( final IOException ex )
                    {
                        m_writeFailure = ex;
                        LOG.debug( "Exception while flushing buffer occurred.", ex );
                    }
                    return;
                }
                writeBuffer( buffer );
            }
        }
        
        
        private void writeBuffer( final Buffer buffer )
        {
            if ( null == m_out )
            {
                return;
            }
            if ( null != m_writeFailure )
            {
                return;
            }
            
            try
            {
                m_out.write( buffer.m_buffer, 0, buffer.m_length );
            }
            catch ( final IOException ex )
            {
                m_writeFailure = ex;
                LOG.debug( "Exception while writing buffer occurred.", ex );
            }
        }
        
        
        private volatile IOException m_writeFailure;
        private volatile long m_offset;
        private volatile long m_length;
        
        private final List< LongRange > m_byteRanges;
        private final CountDownLatch m_readyLatch = new CountDownLatch( 1 );
        private final ArrayBlockingQueue< Buffer > m_buffers;
        private final OutputStream m_out;
    } // end inner class def
    
    
    private final static class ChecksumBufferProcessor implements Runnable
    {
        private ChecksumBufferProcessor( final int numberOfBuffers, final ChecksumComputer computer )
        {
            m_computer = computer;
            m_buffers = new ArrayBlockingQueue<>( numberOfBuffers );
        }
        
        
        public void run()
        {
            m_readyLatch.countDown();
            while ( true )
            {
                try
                {
                    final Buffer buffer = m_buffers.take();
                    if ( DONE_TOKEN == buffer )
                    {
                        return;
                    }
                    
                    m_computer.processBuffer( buffer );
                }
                catch ( final InterruptedException ex )
                {
                    throw new RuntimeException( ex );
                }
            }
        }
        
        
        private final ChecksumComputer m_computer;
        private final ArrayBlockingQueue< Buffer > m_buffers;
        private final CountDownLatch m_readyLatch = new CountDownLatch( 1 );
    } // end inner class def
    
    
    private interface ChecksumComputer
    {
        /**
         * 
         * @param buffer to be processed. Its internal length changes so use the digest update method 
         * with the offset and length parameters.
         */
        void processBuffer( final Buffer buffer );
        
        
        byte [] getChecksum();
    } // end inner class def
    
    
    private final static class Crc32ChecksumComputer implements ChecksumComputer
    {
        private Crc32ChecksumComputer( final String crcClassName )
        {
            try
            {
                final Constructor< ? > con = Class.forName( crcClassName ).getDeclaredConstructor();
                con.setAccessible( true );
                m_crc = (Checksum)con.newInstance();
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
        }
        
        
        public void processBuffer( final Buffer buffer )
        {
            m_crc.update( buffer.m_buffer, 0, buffer.m_length );
        }
        
        
        /*
         * CRC32 checksums are calculated as a 32-bit value which must be
         * converted to a byte array.  Because Java is always big-endian,
         * the value returned is always in network byte order, which means
         * it is consistent with non-CRC32 checksums.  Tests enforce this.
         */
        public byte [] getChecksum()
        {
            final ByteBuffer buffer = ByteBuffer.allocate( 8 );
            buffer.order( ByteOrder.BIG_ENDIAN );
            buffer.putLong( m_crc.getValue() );
            
            final byte [] array = buffer.array();
            if ( 0 < array[ 0 ] || 0 < array[ 1 ] || 0 < array[ 2 ] || 0 < array[ 3 ] )
            {
                throw new RuntimeException( 
                        "Checksum should be 32 bits, so how did it spill into the upper 4 bytes?" );
            }
            
            final byte [] retval = new byte[ 4 ];
            System.arraycopy( array, 4, retval, 0, 4 );
            return retval;
        }
        
        
        private final Checksum m_crc;
    } // end inner class def
    
    
    private final static class CryptographicChecksumComputer implements ChecksumComputer
    {
        private CryptographicChecksumComputer( final String algorithm )
        {
            try
            {
                m_messageDigest = MessageDigest.getInstance( algorithm );
            }
            catch ( final NoSuchAlgorithmException ex )
            {
                throw new RuntimeException( "Algorithm '" + algorithm + "' isn't supported.", ex );
            }
        }
        
        
        public void processBuffer( final Buffer buffer )
        {
            m_messageDigest.update( buffer.m_buffer, 0, buffer.m_length );
        }
        
        
        public byte [] getChecksum()
        {
            return m_messageDigest.digest();
        }
        
        
        private final MessageDigest m_messageDigest;
    } // end inner class def


    private final static class FastMD5ChecksumComputer implements ChecksumComputer
    {
        private FastMD5ChecksumComputer()
        {            
            m_fastMD5 = new FastMD5();
        }
       

        public void processBuffer( final Buffer buffer )
        {
            m_fastMD5.update( buffer.m_buffer, 0, buffer.m_length );
        }


        public byte [] getChecksum()
        {
            return m_fastMD5.digestAndReset();
        }

        
        private final FastMD5 m_fastMD5;
    } // end inner class def
    
    
    private final static class Buffer
    {
        private Buffer( final byte [] buffer )
        {
            m_buffer = buffer;
        }
        
        private Buffer()
        {
            m_buffer = null;
        }
        
        private volatile int m_length;
        private final byte [] m_buffer;
    } // end inner class def
    
    
    public byte [] getChecksum()
    {
        if ( null == m_checksum )
        {
            throw new IllegalStateException(
                    "Checksum not calculated yet, or no checksum was ever computed." );
        }
        return m_checksum.clone();
    }
    
    
    public long getTotalBytesMoved()
    {
        return m_totalBytesRead;
    }
    
    
    public interface BytesReadListener
    {
        void bytesRead( final int numberOfBytes );
    } // end inner class def
    
    
    private volatile long m_totalBytesRead;
    private volatile byte [] m_checksum;
    private final BytesReadListener m_bytesReadListener;
    private final String m_dataTransferName;
    private final long m_expectedTotalBytes;
    private final ByteRanges m_byteRanges;
    private final OutputStream m_os;
    private final InputStreamProvider m_inputProvider;
    private final ChecksumType m_checksumAlgorithm;
    private final AtomicBoolean m_runCalled = new AtomicBoolean( false );
    private final int m_writeBufferSize;
    private final int m_numberOfWriteBuffers;
    
    private int m_readBufferOffset;
    private int m_readBufferLength;
    private final byte [] m_readBuffer;
    
    private final static int MAX_BUFFER_CAPACITY = 6 * 1024 * 1024;
    private final static int MAX_NUMBER_OF_BUFFERS = 128;
    private final static Buffer DONE_TOKEN = new Buffer();
    private final static BytesRenderer BYTES_RENDERER = new BytesRenderer();
    private final static Logger LOG = Logger.getLogger( ThreadedDataMover.class );

    
    /**
     * This init approach is used to work most easily in conjunction with test
     * run thread lead detection and prevention. Please don't change it unless
     * you're explicitly working on test run thread leak detection/prevention.
     */
    private static void initWorkPools()
    {
        synchronized( WORK_POOLS_LOCK )
        {
            if ( null == s_writeDataWP || s_writeDataWP.isShutdown() )
            {
                s_writeDataWP = WorkPoolFactory.createWorkPool(
                    Runtime.getRuntime().availableProcessors() * 2,
                    ThreadedDataMover.class.getSimpleName() + "-DataWriter" );
                s_checksumWP = WorkPoolFactory.createWorkPool(
                    Runtime.getRuntime().availableProcessors(),
                    ThreadedDataMover.class.getSimpleName() + "-ChecksumComputer" );
            }
        }
    }
    
    private static final Object WORK_POOLS_LOCK = new Object();
    private static WorkPool s_writeDataWP = null;
    private static WorkPool s_checksumWP = null;
}
