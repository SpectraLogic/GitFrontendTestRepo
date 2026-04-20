/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.codegen.cli;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.spectralogic.util.db.codegen.CMacroCodeGenerator;
import com.spectralogic.util.db.codegen.SqlCodeGenerator;
import com.spectralogic.util.db.manager.postgres.PostgresDataManager;
import com.spectralogic.util.lang.Platform;
import org.apache.log4j.helpers.LogLog;

public final class GenerateCode implements Runnable
{
    public static void main( String[] args )
    {
        if ( 3 > args.length )
        {
            throw new IllegalArgumentException( "" +
                    "Requires 3 arguments: the domains seed class, then the SQL output file, " 
                    + "then the C output directory" );
        }
        new GenerateCode( args[ args.length - 3 ], args[ args.length - 2 ], args[ args.length - 1 ] ).run();
    }
    
    
    public GenerateCode( final String seedClass, final String sqlOutputFile, final String cOutputDir )
    {
        m_sqlOutputFile = new File( sqlOutputFile );
        m_cOutputDir = new File( cOutputDir );
        
        try
        {
            m_seedClass = Class.forName( seedClass );
        }
        catch ( final ClassNotFoundException ex )
        {
            throw new RuntimeException( ex );
        }
    }

    
    public void run()
    {
        log( "File to write sql to: " + m_sqlOutputFile );
        final Set< Class< ? > > seeds = new HashSet<>();
        seeds.add( m_seedClass );
        final PostgresDataManager manager = new PostgresDataManager( 3, seeds );
        
        log( "Generating sql..." );
        final SqlCodeGenerator sqlGenerator = 
                new SqlCodeGenerator( manager.getSupportedTypes(), "Administrator" );
        final String sql = sqlGenerator.getGeneratedCode().getCodeFiles().get( null );

        m_sqlOutputFile.getParentFile().mkdirs();

        log( "Writing sql to file..." );
        try
        {
            final FileWriter writer = new FileWriter( m_sqlOutputFile );
            writer.write( sql );
            writer.close();
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
        log( "Database generated successfully." );
        
        log( "Directory to write c to: " + m_cOutputDir );
        m_cOutputDir.mkdirs();

        try {
            log("Generating c...");
            final CMacroCodeGenerator cGenerator =
                    new CMacroCodeGenerator(manager.getSupportedTypes());
            final Map<String, String> c = cGenerator.getGeneratedCode().getCodeFiles();

            log("Writing c to files...");
            for (final Map.Entry<String, String> e : c.entrySet()) {
                try {
                    final FileWriter writer = new FileWriter(
                            m_cOutputDir.getAbsolutePath() + Platform.FILE_SEPARATOR + e.getKey());
                    writer.write(e.getValue());
                    writer.close();
                } catch (final Exception ex) {
                    throw new RuntimeException(ex);
                }
            }

            log("C generated successfully.");
        } catch (final Exception ex) {
            log("Failed to generate C code: " + ex.getMessage());
        }

        log( "Code generator SUCCEEDED." );
    }
    
    
    private void log( final String line )
    {
        final PrintStream out = System.out;
        out.println( line );
    }
    
    
    private final File m_sqlOutputFile;
    private final File m_cOutputDir;
    private final Class< ? > m_seedClass;
}
