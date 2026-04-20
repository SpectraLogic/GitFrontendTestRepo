/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.codegen;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.db.manager.postgres.PostgresDataManager;
import com.spectralogic.util.db.mockdomain.County;
import com.spectralogic.util.lang.Platform;

public final class SqlCodeGenerator_Test 
{
    @Test
    public void testGenerateDatabaseCreationSqlScriptWorks() 
            throws IOException
    {
        final Set< Class< ? > > seeds = new HashSet<>();
        seeds.add( County.class );
        
        String sql = 
                new SqlCodeGenerator( new PostgresDataManager( 4, seeds ).getSupportedTypes(), "dbuser" )
                .getGeneratedCode().getCodeFiles().get( null );
        sql = compact( sql );
        
        final InputStream in = getClass().getResourceAsStream( "/expectedsqlgenerationcode.props" );
        final Properties properties = new Properties();
        properties.load( in );
        
        final int max = Integer.valueOf( properties.getProperty( "expected.max" ) ).intValue();
        if ( 1 > max )
        {
            throw new RuntimeException( "Failed to load expected.max" );
        }
        for ( int i = 0; i <= max; ++i )
        {
            String expected = properties.getProperty( "expected." + i );
            if ( 40 > expected.length() )
            {
                throw new RuntimeException( "Did this load properly? " + expected );
            }
            expected = compact( expected );
            assertTrue(
                    sql.contains( expected ),
                    "Sql generated (" + sql + ") shoulda included substring: " + expected);
        }
    }
    
    
    private String compact( String sql )
    {
        sql = sql.replace( "  ", " " );
        sql = sql.replace( Platform.NEWLINE, "" );
        sql = sql.replace( "( ", "(" );
        sql = sql.replace( " )", ")" );
        sql = sql.replace( ", ", "," );
        
        if ( sql.contains( "  " ) )
        {
            return compact( sql );
        }
        return sql;
    }
}
