/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.aws;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class S3HeaderType_Test 
{

    @Test
    public void testFromHeaderNameNullNameNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {

    public void test() throws Throwable
            {
                S3HeaderType.fromHeaderName( null );
            }
        } );
    }
    
    
    @Test
    public void testFromHeaderNameUnrecognizedNameReturnsNull()
    {
        assertNull(S3HeaderType.fromHeaderName( "OOPSIES" ), "Shoulda returned null since does not exist.");
    }
    
    
    @Test
    public void testFromHeaderNameReturnsHeader()
    {
        assertNotNull(S3HeaderType.fromHeaderName( "content-length" ), "Shoulda returned S3 header type instance.");
        assertNotNull(S3HeaderType.fromHeaderName( "content-Length" ), "Shoulda returned S3 header type instance.");
        assertNotNull(S3HeaderType.fromHeaderName( "CONTENT-length" ), "Shoulda returned S3 header type instance.");
    }
    
    
    @Test
    public void testEnumsSortedAlphabetically()
    {
        String previous = "";
        for ( final S3HeaderType header : S3HeaderType.values() )
        {
            if ( 0 > header.name().compareTo( previous ) )
            {
                fail( previous + " comes after " + header.name() + "." );
            }
            previous = header.name();
        }
    }
}
