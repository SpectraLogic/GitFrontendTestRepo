/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.lang;

import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.log4j.Logger;

public final class HardwareInformationProvider
{
    public static String getSerialNumber()
    {
        synchronized ( LOCK )
        {
            if ( null != s_serialNumber )
            {
                return s_serialNumber;
            }
            s_serialNumber = calculateSerialNumber();
            return s_serialNumber;
        }
    }
    
    
    private static String calculateSerialNumber()
    {
        BufferedReader reader = null;
        try
        {
            reader = new BufferedReader( new FileReader( "/etc/spectra/serial_number" ) );
            return reader.readLine().trim();
        }
        catch ( final Exception ex )
        {
            LOG.error( "Failed to determine system serial number.", ex );
            return "UNKNOWN";
        }
        finally
        {
            if ( null != reader )
            {
                try
                {
                    reader.close();
                }
                catch ( final Exception ex )
                {
                    LOG.warn( "Failed to close serial number file reader.", ex );
                }
            }
        }
    }
    
    
    public static int getZfsCacheFilesystemRecordSize()
    {
        return ZFS_CACHE_FILESYSTEM_RECORD_SIZE;
    }
    
    
    public static int getTomcatBufferSize()
    {
        return TOMCAT_BUFFER_SIZE;
    }
    
    
    private static String s_serialNumber;
    private final static int ZFS_CACHE_FILESYSTEM_RECORD_SIZE = 512 * 1024;
    private final static int TOMCAT_BUFFER_SIZE = 8 * 1024;
    private final static Object LOCK = new Object();
    private final static Logger LOG = Logger.getLogger( HardwareInformationProvider.class );
}
