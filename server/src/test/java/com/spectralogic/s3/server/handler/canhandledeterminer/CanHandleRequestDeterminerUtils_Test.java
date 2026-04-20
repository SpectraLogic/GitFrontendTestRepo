/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.canhandledeterminer;


import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class CanHandleRequestDeterminerUtils_Test
{
    @Test
    public void testSanitizeSampleUrlNullUrlNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                CanHandleRequestDeterminerUtils.sanitizeSampleUrl( null );
            }
        } );
    }
    

    @Test
    public void testSanitizeSampleUrlWithoutQuestionMarksReturnsOriginalUrl()
    {
        assertEquals("http://hello.html", CanHandleRequestDeterminerUtils.sanitizeSampleUrl( "http://hello.html" ), "Shoulda returned sanitized url.");
    }
    

    @Test
    public void testSanitizeSampleUrlWithSingleQuestionMarkReturnsOriginalUrl()
    {
        assertEquals("http://hello.html?op=a&be=c", CanHandleRequestDeterminerUtils.sanitizeSampleUrl( "http://hello.html?op=a&be=c" ), "Shoulda returned sanitized url.");
        assertEquals("http://hello.html?op=a", CanHandleRequestDeterminerUtils.sanitizeSampleUrl( "http://hello.html?op=a" ), "Shoulda returned sanitized url.");
    }
    

    @Test
    public void testSanitizeSampleUrlWithMultipleQuestionMarksReturnsModifiedOriginalUrlWithAmperstands()
    {
        assertEquals("http://hello.html?op=a&ap=b", CanHandleRequestDeterminerUtils.sanitizeSampleUrl( "http://hello.html?op=a?ap=b" ), "Shoulda returned sanitized url.");
        assertEquals("http://hello.html?op=a&ap=b&c=d&d=e", CanHandleRequestDeterminerUtils.sanitizeSampleUrl( "http://hello.html?op=a?ap=b&c=d?d=e" ), "Shoulda returned sanitized url.");
    }
}
