/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.lang;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.marshal.JsonMarshaler;

public class RuntimeInformationLogger
{
    public static void logRuntimeInformation()
    {
        if ( LOGGED.getAndSet( true ) )
        {
            return;
        }
        
        StringBuilder msg = new StringBuilder();
        msg.append( "JVM initialized.  Runtime information is as follows..." + Platform.NEWLINE );
        msg.append( getPropertyLine( "User Name", "user.name" ) );
        msg.append( getPropertyLine( "Temp Directory", "java.io.tmpdir" ) );
        msg.append( getPropertyLine( "File Encoding", "file.encoding" ) );
        msg.append( getLine( "System Serial Number", HardwareInformationProvider.getSerialNumber() ) );
        msg.append( getPropertyLine( "JVM Architecture", "sun.arch.data.model", "-bit" ) );
        msg.append( getPropertyLine( "JVM Version", "java.version" ) );
        msg.append( getPropertyLine( "JVM Home", "java.home" ) );
        msg.append( getLine( 
                "JVM Maximum Number of Processors", 
                String.valueOf( Runtime.getRuntime().availableProcessors() ) ) );
        msg.append( getLine( 
                "JVM Maximum Memory", 
                (int)( Runtime.getRuntime().maxMemory() / ( Math.pow( 1024, 2 ) ) ) + " MB" ) );
        msg.append( getPropertyLine( "JVM Classpath", "java.class.path" ) );
        LOG.info( msg );

        msg = new StringBuilder();
        msg.append( "Build version information:" + Platform.NEWLINE 
                    + JsonMarshaler.formatPretty( 
                            ConfigurationInformationProvider.getInstance().getBuildInformation().toJson() ) );
        LOG.info( msg );
    }
    
    
    private static String getPropertyLine(
            final String description,
            final String property )
    {
        return getLine( description, System.getProperty( property ) );
    }
    
    
    private static String getPropertyLine(
            final String description,
            final String property, 
            final String suffix )
    {
        return getLine( description, System.getProperty( property ) + suffix );
    }
    
    
    private static String getLine( final String description, final String value )
    {
        return "  " + description + ": " + value + Platform.NEWLINE;
    }
    

    private final static AtomicBoolean LOGGED = new AtomicBoolean();
    private final static Logger LOG = Logger.getLogger( RuntimeInformationLogger.class );
}
