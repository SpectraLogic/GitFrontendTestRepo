/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.spectralogic.util.io.lang.InputStreamProvider;
import com.spectralogic.util.lang.Validations;

/**
 * An {@link InputStreamProvider} that will never return the next {@link InputStream} until the backing file
 * for the previous {@link InputStream} has been deleted, so that the maximum temporary extra space consumed
 * never exceeds a single source file size.
 */
public final class FileDeletingInputStreamProvider implements InputStreamProvider
{
    public FileDeletingInputStreamProvider( final List< File > files )
    {
        Validations.verifyNotNull( "Files", files );
        if ( files.isEmpty() )
        {
            throw new IllegalArgumentException( "Must specify at least one file." );
        }
        
        m_files = new ArrayList<>( files );
    }

    
    synchronized public InputStream getNextInputStream()
    {
        if ( null != m_fileBeingStreamed )
        {
            try
            {
                m_currentInputStream.close();
            }
            catch ( final IOException ex )
            {
                LOG.warn( "Failed to close input stream for " + m_fileBeingStreamed + ".", ex );
            }
            if ( !m_fileBeingStreamed.delete() )
            {
                LOG.warn( "Failed to delete: " + m_fileBeingStreamed );
            }
            m_fileBeingStreamed = null;
            LOCK.unlock();
        }
        if ( m_files.isEmpty() )
        {
            return null;
        }
        
        try
        {
            LOCK.lockInterruptibly();
        }
        catch ( final InterruptedException ex )
        {
            throw new RuntimeException( ex );
        }
        
        m_fileBeingStreamed = m_files.remove( 0 );
        try
        {
            m_currentInputStream = new FileInputStream( m_fileBeingStreamed );
            return m_currentInputStream;
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    private File m_fileBeingStreamed;
    private InputStream m_currentInputStream;
    private final List< File > m_files;
    
    private final static ReentrantLock LOCK = new ReentrantLock();
    private final static Logger LOG = Logger.getLogger( FileDeletingInputStreamProvider.class );
}
