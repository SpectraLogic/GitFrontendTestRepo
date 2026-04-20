/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.io;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class SingleInputStreamProvider_Test 
{
    @Test
    public void testConstructorNullInputStreamNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new SingleInputStreamProvider( null );
            }
            } );
    }
    
    
    @Test
    public void testGetNextInputStreamReturnsAppropriately()
    {
        final InputStream is = new ByteArrayInputStream( new byte[ 1 ] );
        final SingleInputStreamProvider provider = new SingleInputStreamProvider( is );
        assertEquals(
                is,
                provider.getNextInputStream(),
                "Shoulda returned the single input stream."
                 );
        assertEquals(
                null,
                provider.getNextInputStream(),
                "Shoulda indicated no additional input streams."
                );
        assertEquals(
                null,
                provider.getNextInputStream(),
                "Shoulda indicated no additional input streams."
                );
    }
}
