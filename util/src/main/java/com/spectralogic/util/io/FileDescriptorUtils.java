package com.spectralogic.util.io;
import java.io.FileDescriptor;
import java.lang.reflect.Field;

import org.apache.log4j.Logger;

public final class FileDescriptorUtils 
{
    public static int getFileDescriptorNative( final FileDescriptor fd ) 
    {
        try 
        {
            return s_fd.getInt( fd );
        }
        catch ( final Exception e ) 
        {
            LOG.debug( "Failed to determine native file descriptor.", e );
        }
        return -1;
    }
    
    
    private final static Logger LOG = Logger.getLogger( FileDescriptorUtils.class );
    private static Field s_fd;
    static 
    {
        try 
        {
            s_fd = FileDescriptor.class.getDeclaredField( "fd" );
            s_fd.setAccessible( true );
        } 
        catch ( final Exception ex ) 
        {
            LOG.debug( "Failed to get file descriptor field.", ex );
            s_fd = null;
        }
    }
}
