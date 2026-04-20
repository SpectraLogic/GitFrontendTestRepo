/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.testfrmwrk;


import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.log4j.Logger;

import com.spectralogic.util.db.test.TestDataSource;


/** */
public class TestDataSource_Test 
{
    @Test
    public void testNullConstructorParamsNotAllowed()
    {
        boolean thrown = false;
        try
        {
            new TestDataSource( null, "foo", "bar", "boo" );
        }
        catch( final IllegalArgumentException iae )
        {
            LOG.error( iae );
            thrown = true;
        }
        assertTrue(
                thrown,
                "Null server name shouldn't have been allowed." );
        
        thrown = false;
        try
        {
            new TestDataSource( "foo", null, "bar", "boo" );
        }
        catch( final IllegalArgumentException iae )
        {
            LOG.error( iae );
            thrown = true;
        }
        assertTrue(
                thrown,
                "Null DB name shouldn't have been allowed." );
        
        thrown = false;
        try
        {
            new TestDataSource( "foo", "bar", null, "boo" );
        }
        catch( final IllegalArgumentException iae )
        {
            LOG.error( iae );
            thrown = true;
        }
        assertTrue(
                thrown,
                "Null DB username shouldn't have been allowed."  );
    }
    
    private static final Logger LOG = Logger.getLogger( TestDataSource.class ); 
}
