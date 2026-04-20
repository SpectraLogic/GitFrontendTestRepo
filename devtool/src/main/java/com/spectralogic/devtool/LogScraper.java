/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.devtool;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;


public final class LogScraper
{  
    public static void main( String[] args ) // $codepro.audit.disable illegalMainMethod
    {
        final File file = new File( "c:\\del\\xxx" );
        final List< String > dataPlannerLogs = getFiles( file.getPath(), 
                new String [ ] { "dataplanner" }, false );

        final List< String > fileNamelist = getFiles( file.getPath(), 
                new String [ ] { "tomcat.server-http" }, false );

        out( dataPlannerLogs.toString().replaceAll( ",", NL ) );
        out( fileNamelist.toString().replaceAll( ",", NL ) );        
    }


    static List< String > getFiles( final String directoryName, final String[] endsWith, 
            final boolean mustBeWritable ) 
            {
        final List< String > retFileNameList = new ArrayList<>();
        final File directory = new File( directoryName );
        final File[] fList = directory.listFiles();

        for ( final File file : fList ) 
        {
            final boolean isInBuildDir = file.getPath().contains( "\\build\\" ) 
                    || file.getPath().contains( "/build/" );
            if ( isInBuildDir )
            {
                continue;
            }
            if ( file.isFile() ) 
            {
                final boolean fileEndsWith = file.getName().endsWith( endsWith[ 0 ] );
                final boolean selectFile = ( mustBeWritable )? file.canWrite() 
                        && fileEndsWith:fileEndsWith;                
                if ( selectFile )
                {
                    retFileNameList.add( file.getAbsolutePath() );
                }
            }
            else if ( file.isDirectory() )
            {
                retFileNameList.addAll( getFiles( file.getAbsolutePath(), endsWith, mustBeWritable ) );
            }
        }

        return retFileNameList;
    }
    
    
    static public void out( final String str )
    {        
        s_bufLogOut.append( str );
        s_pauseOutputLineCounter+= StringUtils.countMatches( str, NL );

        try
        {
            if ( s_pauseOutputLineCounter > PAUSE_OUTPUT_EVERY_LINES )
            {
                S_LOG.info( s_bufLogOut );
                System.in.read();
                s_pauseOutputLineCounter = 0;
                s_bufLogOut = new StringBuilder( NL );
            }
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( ex );
        }
    }


    private static final String NL = System.getProperty( "line.separator" );    
    private static int s_pauseOutputLineCounter = 0;
    private static StringBuilder s_bufLogOut = new StringBuilder();
    private static final int PAUSE_OUTPUT_EVERY_LINES = Integer.MAX_VALUE;
    private static final Logger S_LOG = Logger.getLogger( LogScraper.class );
}