/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.io.lang;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.render.BytesRenderer;


public final class FileUtil
{
    private FileUtil()
    {
        // singleton
    }


    public static void unJarSetExecuteCleanDestDirIfErrors( 
            final File jarPath, 
            final File destDir )
    {    
        
        if ( jarPath == null )
        {
            throw new RuntimeException( "'jarPath' parameter is null." 
                    + " Other parameters: destDir:" + destDir + "." );
        }
        if ( !jarPath.exists() )
        {
            throw new RuntimeException( "jarPath:" + jarPath + ", does not exist." 
                    + " Other parameters: destDir:" + destDir + "." );
        }
        if ( destDir != null && !destDir.exists() )
        {
            destDir.mkdirs();
        }
        if ( destDir == null || !destDir.exists() )
        {
            throw new RuntimeException( "destDir is null, or it does not exist and could not be created" );
        }
        LOG.info( "Starting to unjar: " + getCanonicalPath( jarPath ) 
                + "; in dir: " + getCanonicalPath( destDir ) + "..." );
        try ( final JarFile jarFile = new JarFile( jarPath ) )
        {
            final Enumeration<JarEntry> jarEntries = jarFile.entries();
            while ( jarEntries.hasMoreElements() ) 
            {
                final JarEntry jarEntry = jarEntries.nextElement();
                final File newFileEntry = new File( destDir, jarEntry.getName() );
                if ( !jarEntry.isDirectory() )
                {
                    if ( !newFileEntry.exists() )
                    {
                        try ( final InputStream is = jarFile.getInputStream( jarEntry ) )
                        {
                            FileUtils.copyInputStreamToFile( is, newFileEntry );
                        }
                        final boolean execSetOk = newFileEntry.setExecutable( true, false );
                        final boolean readSetOk = newFileEntry.setReadable( true, false );
                        if ( !execSetOk || !readSetOk )
                        {
                            LOG.info( "We could not set some flags for file " 
                                    + getCanonicalPath( newFileEntry ) 
                                    + ", execSetOk =" + execSetOk + ", readSetOk =" + readSetOk );
                        }
                        if ( !newFileEntry.exists() )
                        {
                            LOG.info( "Exists flag is false for the file we just created "
                                    + newFileEntry.getName() + " with path " 
                                    + getCanonicalPath( newFileEntry ) );
                        }
                    }
                }
                else
                {
                    if ( !newFileEntry.exists() )
                    {
                        newFileEntry.mkdir();
                    }
                }
            }
        }
        catch ( final IOException ex )
        {      
            try
            {
                FileUtils.cleanDirectory( destDir );
            }
            catch ( final Exception ex2 ) 
            {
                LOG.warn( "Unjarring failed so we tried to clean up the temp files we created.", ex2 );
            }
            
            throw new RuntimeException( "Error while unjarring jar=" 
                    + getCanonicalPath( jarPath ) + " to dir="
                    + destDir + ", message=" + ex.getMessage(), ex );
        }
        LOG.info( "Finished to unjar '" + getCanonicalPath( jarPath ) 
                + "' in dir " + getCanonicalPath( destDir ) + "." );
    }

    
    static public String getCanonicalPath( final File f )
    {
        if ( f == null )
        {
            return "null file object";
        }
        if ( !f.exists() )
        {
            return "File " + f.getName() + " does not exist";
        }
        try
        {
            return f.getCanonicalPath();
        }
        catch ( final Exception ex ) 
        {
            return "getCanonicalPath for " + f.getName() + " throws error=" + ex.getMessage();
        }
    }
    
    
    /**
     * Wrapper for the Apache FileUtils.cleanDirectory without handling exceptions
     * 
     * @param tmpDir subfolder to clean
     */
    public static void cleanDirectoryQuietly( final File tmpDir )
    {
        try
        {
            FileUtils.cleanDirectory( tmpDir );
        }
        catch ( final Exception ex ) 
        {
            LOG.debug( "Trying to clean up folders but not dealing with errors." + ex.getMessage() );
        }
    }


    public static File getTempDirCheckWrite()
    {
        final String javaTmpdir = System.getProperty( "java.io.tmpdir" );
        final File tmpDir = new File( javaTmpdir );
        if ( tmpDir.exists() && tmpDir.canWrite() )
        {
            return tmpDir;
        }
        return null;
    }


    /**
     * @return true if successful
     */
    public static boolean compressFilesIntoZipFile( 
            final File destination, 
            final Set< File > sourceFiles )
    {
        if ( null == destination )
        {
            throw new IllegalArgumentException( "Destination file cannot be null." ); 
        }
        if ( null == sourceFiles || sourceFiles.isEmpty() )
        {
            throw new IllegalArgumentException( "Source files cannot be null or empty." ); 
        }

        boolean success = true;

        final BytesRenderer bytesRenderer = new BytesRenderer();
        final int bufferSize = 4096;

        long bytesOfUncompressedData = 0;
        ZipOutputStream zipOutStream = null;
        final Duration duration = new Duration();
        try 
        {
            zipOutStream = new ZipOutputStream( new BufferedOutputStream( 
                    new FileOutputStream( destination ) ) );
            zipOutStream.setLevel( Deflater.BEST_COMPRESSION );

            final byte [] data = new byte[ bufferSize ];
            for ( final File file : sourceFiles )
            {
                LOG.info( "Compressing " + file + " (" + bytesRenderer.render( file.length() ) + ")..." );
                final BufferedInputStream origin =
                        new BufferedInputStream( new FileInputStream( file ), bufferSize );
                final ZipEntry entry = new ZipEntry( file.getName() );
                zipOutStream.putNextEntry( entry );
                int count = 0;
                while ( -1 != count ) 
                {
                    count = origin.read( data, 0, bufferSize );
                    if ( -1 != count )
                    {
                        bytesOfUncompressedData += count;
                        zipOutStream.write( data, 0, count );
                    }
                }
                origin.close();
                zipOutStream.closeEntry();
            }
        }
        catch ( final IOException ex ) 
        {
            throw new RuntimeException( "Failed to compress logs.", ex ); 
        }
        finally
        {
            success &= closeChannel( zipOutStream );
        }

        LOG.info( "Compressed " 
                + sourceFiles.size() 
                + " files in " 
                + duration 
                + ".  " 
                + bytesRenderer.render( bytesOfUncompressedData ) 
                + " was compressed down to a " 
                + bytesRenderer.render( destination.length() ) 
                + " zip file: " 
                + destination ); 

        return success;
    }


    /**
     * @return true if successful
     */
    private static boolean closeChannel( final Closeable channel )
    {
        boolean success = true;
        try
        {
            if ( null != channel )
            {
                channel.close();                    
            }
        }
        catch ( final IOException e )
        {
            LOG.warn( "Problem when closing channel.", e ); 
            success = false;
        }

        return success;
    }


    private final static Logger LOG = Logger.getLogger( FileUtil.class );
}
