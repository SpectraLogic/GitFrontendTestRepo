package com.spectralogic.util.io;

import java.io.FileDescriptor;

import org.apache.log4j.Logger;

import com.sun.jna.LastErrorException;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;

public final class CLibrary
{
    private static volatile boolean s_loaded = false;
    private static volatile PosixFadvise s_library = null;
    
    public static final int POSIX_FADV_NOREUSE = 5;
    public static final int POSIX_STDIN_FILENO = 1;
    
    private final static Logger LOG = Logger.getLogger( CLibrary.class );
    
    public interface PosixFadvise extends Library 
    {
        public int posix_fadvise(  // $codepro.audit.disable methodNamingConvention
                int fd, NativeLong offset, NativeLong len, int flag ) throws LastErrorException;
    }

    static 
    {
        try
        {
            s_library = (PosixFadvise) Native.loadLibrary( Platform.C_LIBRARY_NAME, PosixFadvise.class );
            /* 
             * Test to see whether posix_fadvise() is actually supported by the libc.  
             * For systems that have a libc
             * but not posix_fadvise(), this will throw UnsatisfiedLinkError.
             */
            s_library.posix_fadvise( POSIX_STDIN_FILENO,
                new NativeLong( 0 ), new NativeLong( 0 ), POSIX_FADV_NOREUSE );
            s_loaded = true;
        } 
        catch ( final UnsatisfiedLinkError e ) // $codepro.audit.disable rethrownExceptions
        {
            LOG.debug( "OS doesn't support libc/posix_fadvise(), wrapper will do nothing.", e );
        }
    }

    /**
     * Advise the filesystem certain things about how the file's data will be used.  For more information,
     * see (for example) the FreeBSD manual page: http://www.freebsd.org/cgi/man.cgi?posix_fadvise
     */
    public static int fadvise(int fd, NativeLong offset, NativeLong len, int flag) throws LastErrorException
    {
        if ( s_loaded && fd > 0 )
        {
            return ( s_library.posix_fadvise( fd, offset, len, flag ) );
        }
        return ( 0 );
    }
    
    /**
     * Notify the filesystem, where posix_fadvise() is supported, that the data will not be reused. 
     */
    public static int discardDataAfterSingleUse( final FileDescriptor fd )
    {
        return fadvise( FileDescriptorUtils.getFileDescriptorNative( fd ), 
            /*offset*/ new NativeLong( 0 ), /*len*/ new NativeLong( 0 ), POSIX_FADV_NOREUSE);
    }
}