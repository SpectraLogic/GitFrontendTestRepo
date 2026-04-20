/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.io.lang;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.MBeanServer;

import com.spectralogic.util.lang.Platform;
import org.apache.log4j.Logger;

import com.spectralogic.util.lang.CollectionFactory;
import com.sun.management.HotSpotDiagnosticMXBean;

public final class HeapDumper
{
    private HeapDumper()
    {
        // singleton
    }
    

    /**
     * Dumps and zips the heap if this method has not previously been called
     * @return the {@link File} the zip of the heap was created at, or null if no heap was created
     */
    public static File dumpAndZipHeapDueToError()
    {
        LOG.warn( "Error occurred that requires heap dump." );
        if ( DUMPED_DUE_TO_ERROR.getAndSet( true ) )
        {
            LOG.warn( "Will not dump heap since heap already dumped previously due to an error." );
            return null;
        }
        return dumpAndZipHeap( true );
    }
    
    
    /**
     * Dumps and zips the heap no matter what
     * @return the {@link File} the zip of the heap was created at
     */
    public static File dumpAndZipHeap(boolean dueToError)
    {
        if ( ZIP_HEAP_IN_PROGRESS.getAndSet( true ) )
        {
            LOG.info( "Heap dump already in progress.  Ignoring this request." );
            return null;
        }
        
        try
        {
            return dumpAndZipHeapInternal( dueToError );
        }
        finally
        {
            ZIP_HEAP_IN_PROGRESS.set( false );
        }
    }
    
    
    private static File dumpAndZipHeapInternal( boolean dueToError )
    {
        final String logsDir = "/persist/data_path_logs/";

        String fileName = "unspecifiedjavaprocess.hprof"; //default file name in case one isn't specified
        final String errorDumpPath;
        if (JVM_SPECIFIED_HEAP_DUMP_TARGET != null) {
            if (Files.isDirectory(Paths.get(JVM_SPECIFIED_HEAP_DUMP_TARGET))) {
                errorDumpPath = JVM_SPECIFIED_HEAP_DUMP_TARGET + fileName;
            } else {
                fileName = Paths.get(JVM_SPECIFIED_HEAP_DUMP_TARGET).getFileName().toString();
                errorDumpPath = JVM_SPECIFIED_HEAP_DUMP_TARGET;
            }
        } else {
            errorDumpPath = System.getProperty( "java.io.tmpdir" ) + fileName;
        }

        final String target;
        //If this is an explicit dump request (not due to error) we don't want to dump to the normal dump path directory
        if (Files.isDirectory(Paths.get(logsDir)) && !dueToError) {
            target = logsDir + fileName;
        } else {
            target = errorDumpPath;
        }

        final String targetWithTimestamp = injectTimestamp( target );

        final File hprofFile = new File( targetWithTimestamp );
        final File zipFile = new File( targetWithTimestamp + ".zip" );
        HeapDumper.dumpHeap( hprofFile );
        if ( zipFile.exists() )
        {
            zipFile.delete();
        }
        FileUtil.compressFilesIntoZipFile(
                zipFile, 
                CollectionFactory.toSet( hprofFile ) );
        hprofFile.delete();
        
        return zipFile;
    }
    
    
    private static String injectTimestamp( final String target )
    {
        final Calendar cal = new GregorianCalendar();
        final int lastSlash = target.lastIndexOf( Platform.FILE_SEPARATOR );
        final int lastDot = target.lastIndexOf( '.' );
        final String date = String.format( "%04d-%02d-%02d_%02d-%02d-%02d",
                cal.get( Calendar.YEAR ),
                ( cal.get( Calendar.MONTH ) + 1 ),
                cal.get( Calendar.DAY_OF_MONTH ),
                cal.get( Calendar.HOUR_OF_DAY ),
                cal.get( Calendar.MINUTE ),
                cal.get( Calendar.SECOND ) );
        if (lastDot > lastSlash) {
            return target.substring( 0, lastDot ) + "_" + date + target.substring( lastDot );
        } else {
            return target + "_" + date;
        }
    }
    
    
    private static void dumpHeap( final File destination )
    {     
        synchronized ( DUMP_HEAP_LOCK )
        {
            dumpHeapInternal( destination );
        }
    }
    
    
    private static void dumpHeapInternal( final File destination )
    {    
        LOG.info( "Dumping heap to " + destination + "..." );
        if ( destination.exists() )
        {
            destination.delete();
            LOG.info( "Deleted existing heap dump at destination." );
        }
        
        try
        {
            final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            final HotSpotDiagnosticMXBean mxBean = ManagementFactory.newPlatformMXBeanProxy(
                        mbeanServer,
                        "com.sun.management:type=HotSpotDiagnostic",  
                        HotSpotDiagnosticMXBean.class );
        
            mxBean.dumpHeap( 
                    destination.getAbsolutePath(),
                    true ); // only see live objects / we don't care about objects eligible for gc
            LOG.info( "Dumped heap to: " + destination );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException(
                    "Failed to dump heap to " + destination + ".", 
                    ex );
        }
    }
    
    
    private static String getJvmSpecifiedTarget()
    {
        final RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        final List< String > args = runtimeMxBean.getInputArguments();
        if ( !args.contains( HEAP_AUTO_DUMP_ARG_KEY ) )
        {
            LOG.warn( "JVM parameter " + HEAP_AUTO_DUMP_ARG_KEY 
                      + " was not specified.  This JVM param is required to ensure "
                      + OutOfMemoryError.class.getSimpleName() + " errors can be debugged." );
        }
        for ( final String arg : args )
        {
            if ( !arg.startsWith( HEAP_DESTINATION_ARG_KEY ) )
            {
                continue;
            }
            
            final String retval = arg.split( "=" )[ 1 ];
            LOG.info( "JVM parameter " + HEAP_DESTINATION_ARG_KEY + " was specified: " + retval );
            return retval;
        }
        
        LOG.warn( "JVM parameter " + HEAP_DESTINATION_ARG_KEY 
                  + " was not specified.  This JVM param is required to ensure "
                  + OutOfMemoryError.class.getSimpleName() + " errors can be debugged." );
        return null;
    }
    
    
    public static void ensureInitialized()
    {
        // calling this method does nothing other than ensure all static initializers have been executed
    }
    
    
    private final static AtomicBoolean DUMPED_DUE_TO_ERROR = new AtomicBoolean( false );
    private final static Object DUMP_HEAP_LOCK = new Object();
    private final static AtomicBoolean ZIP_HEAP_IN_PROGRESS = new AtomicBoolean( false );
    private final static String HEAP_AUTO_DUMP_ARG_KEY = "-XX:+HeapDumpOnOutOfMemoryError";
    private final static String HEAP_DESTINATION_ARG_KEY = "-XX:HeapDumpPath";
    private final static Logger LOG = Logger.getLogger( HeapDumper.class );
    private final static String JVM_SPECIFIED_HEAP_DUMP_TARGET = getJvmSpecifiedTarget();
}
