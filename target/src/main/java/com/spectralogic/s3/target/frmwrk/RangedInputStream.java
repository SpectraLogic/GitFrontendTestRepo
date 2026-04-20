package com.spectralogic.s3.target.frmwrk;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class RangedInputStream extends FilterInputStream
{
    public RangedInputStream( final InputStream inputStream, final long offset, final long length )
    {
        super( inputStream );
        m_length = length;

        long bytesSkipped;
        try
        {
            bytesSkipped = in.skip( offset );
            if ( offset != bytesSkipped )
            {
                throw new RuntimeException( "Attempted to create input stream from offset " + offset + " but "
                        + "was only able to seek to offset " + bytesSkipped + " bytes.");
            }
        } catch (IOException e)
        {
            throw new RuntimeException( e );
        }
    }


    @Override
    public int read() throws IOException
    {
        if ( m_read >= m_length )
        {
            return -1;
        }
        final int retval = in.read();
        if ( -1 != retval)
        {
            //If we hit this return statement, it means our wrapped stream ended earlier
            //than our range.
            m_read++;
        }
        return retval;
    }


    @Override
    public int read( byte b[] ) throws IOException
    {
        if ( m_read >= m_length )
        {
            return -1;
        }
        final int bytesToRead = (int)Math.min( b.length, ( m_length - m_read ) );
        final int readBytes = in.read( b, 0, bytesToRead );
        if ( -1 == readBytes )
        {
            //If we hit this return statement, it means our wrapped stream ended earlier
            //than our range.
            return -1;
        }
        m_read += readBytes;
        return readBytes;
    }


    @Override
    public int read( byte b[], int off, int len ) throws IOException
    {
        if ( m_read >= m_length )
        {
            return -1;
        }
        final int bytesToRead = (int)Math.min( len, m_length - m_read );
        final int readBytes = in.read( b, off, bytesToRead );
        if ( -1 == readBytes )
        {
            //If we hit this return statement, it means our wrapped stream ended earlier
            //than our range.
            return -1;
        }
        m_read += readBytes;
        return readBytes;
    }


    @Override
    public long skip( long n ) throws IOException
    {
        final long bytesToSkip = Math.min( n, m_length - m_read );
        final long skippedBytes = in.skip( bytesToSkip );
        m_read += skippedBytes;
        return skippedBytes;
    }


    @Override
    public int available() throws IOException
    {
        return (int)Math.min( ( m_length - m_read ), in.available() );
    }


    private final long m_length;
    private long m_read = 0;
}
