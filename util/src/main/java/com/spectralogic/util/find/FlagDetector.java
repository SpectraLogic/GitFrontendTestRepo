/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.find;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.log.LogUtil;

/**
 * Detects and reports on flags that indicate that non-production behavior should occur.
 */
public final class FlagDetector
{
    public static boolean isFlagSet( String flag )
    {
        Validations.verifyNotNull( "Flag", flag );
        flag = flag.toLowerCase();
        synchronized ( CACHE )
        {
            if ( CACHE.containsKey( flag ) )
            {
                return CACHE.get( flag ).booleanValue();
            }
            CACHE.put( flag, determineRunMode( flag ) );
            return isFlagSet( flag );
        }
    }
    
    
    public static File getFlagFile( final String flag )
    {
        return new File( TEMP_DIR + "_FLAG_" + flag );
    }
    
    
    private static Boolean determineRunMode( final String flag )
    {
        Validations.verifyNotNull( "Flag", flag );
        
        final File flagFile = getFlagFile( flag );
        if ( flagFile.exists() )
        {
            LOG.warn( LogUtil.getLogMessageCriticalBlock(
                    "Flag was set: " + flag + " (no flags should be set in production)", 2 ) );
            return Boolean.TRUE;
        }
        LOG.info( "Flag not set: " + flag + "  (no flags should be set in production)" );
        return Boolean.FALSE;
    }
    
    
    private final static Map< String, Boolean > CACHE = new HashMap<>();
    private final static String TEMP_DIR;
    private final static Logger LOG = Logger.getLogger( FlagDetector.class );
    static
    {
        String tempDir = System.getProperty( "java.io.tmpdir" );
        if ( !tempDir.endsWith( Platform.FILE_SEPARATOR ) )
        {
            tempDir += Platform.FILE_SEPARATOR;
        }
        TEMP_DIR = tempDir;
        LOG.info( "Flags must be located at: " + TEMP_DIR );
    }
}
