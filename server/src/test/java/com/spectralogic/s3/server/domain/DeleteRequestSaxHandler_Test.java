/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.domain;

import java.nio.charset.Charset;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.IOUtils;

import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.util.marshal.sax.SaxException;
import com.spectralogic.util.marshal.sax.SaxParser;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class DeleteRequestSaxHandler_Test
{

    @Test
    public void testParseResultReturnsCorrectObjectKeys() throws SaxException
    {
        final S3ObjectsToDeleteApiBean deleteRequest = runParser( TEST_DOCUMENT );
        assertNotNull(deleteRequest, "Shoulda returned an actual value.");
        final List< S3ObjectToDeleteApiBean > objectsToDelete = deleteRequest.getObjectsToDelete();
        assertEquals(2,  objectsToDelete.size(), "Shoulda had two elements.");
        assertEquals("sample1.txt", objectsToDelete.get( 0 ).getKey(), "Shoulda had the first object key name set correctly.");
        assertEquals("sample2.txt", objectsToDelete.get( 1 ).getKey(), "Shoulda had the first object key name set correctly.");
    }
    
    
     @Test
    public void testParseResultReturnsNotQuietWhenNodeNotSpecified() throws SaxException
    {
        final S3ObjectsToDeleteApiBean deleteRequest = runParser( TEST_DOCUMENT );
        assertNotNull(deleteRequest, "Shoulda returned an actual value.");
        assertFalse( deleteRequest.isQuiet(),"Shoulda returned not quiet." );
    }
    
    
     @Test
    public void testParseResultReturnsQuietWhenNodeSpecified() throws SaxException
    {
        final S3ObjectsToDeleteApiBean deleteRequest = runParser( QUIET_TEST_DOCUMENT );
        assertNotNull(deleteRequest, "Shoulda returned an actual value.");
        assertTrue(deleteRequest.isQuiet(), "Shoulda returned quiet.");
    }
    
    
     @Test
    public void testDeleteRequestSaxHandlerInvalidNestingNotAllowed()
    {
        TestUtil.assertThrows(
                "Shoulda thrown an s3 exception because the nodes were out of order.",
                S3RestException.class,
                new BlastContainer()
                {
                     @Test
    public void test() throws SaxException
                    {
                        runParser( INVALID_NESTING_TEST_DOCUMENT );
                    }
                } );
    }
    
    
     @Test
    public void testDeleteRequestSaxHandlerInvalidElementNotAllowed()
    {
        TestUtil.assertThrows(
                "Shoulda thrown an s3 exception because there was an invalid node.",
                S3RestException.class,
                new BlastContainer()
                {
                     @Test
    public void test() throws SaxException
                    {
                        runParser( INVALID_ELEMENT_TEST_DOCUMENT );
                    }
                } );
    }


    private static S3ObjectsToDeleteApiBean runParser( final String document ) throws SaxException
    {
        final S3ObjectsToDeleteApiBeanSaxHandler deleteRequestSaxHandler =
                new S3ObjectsToDeleteApiBeanSaxHandler();
        final SaxParser saxParser = new SaxParser( deleteRequestSaxHandler );
        saxParser.setInputStream( IOUtils.toInputStream( document, Charset.defaultCharset() ) );
        saxParser.parse();
        return deleteRequestSaxHandler.getDeleteRequest();
    }
    

    private static final String TEST_DOCUMENT =
            "<Delete>" +
            "<Object><Key>sample1.txt</Key><VersionId>" + UUID.randomUUID() + "</VersionId></Object>" +
            "<Object><Key>sample2.txt</Key><VersionId>" + UUID.randomUUID() + "</VersionId></Object>" +
            "</Delete>";
    private static final String QUIET_TEST_DOCUMENT =
            "<Delete>" +
            "<Quiet>true</Quiet>" +
            "<Object><Key>sample1.txt</Key></Object>" +
            "<Object><Key>sample2.txt</Key></Object>" +
            "</Delete>";
    private static final String INVALID_NESTING_TEST_DOCUMENT =
            "<Delete>" +
            "<Object><Key><Key>sample1.txt</Key></Key></Object>" +
            "<Object><Key>sample2.txt</Key></Object>" +
            "</Delete>";
    private static final String INVALID_ELEMENT_TEST_DOCUMENT =
            "<Delete>" +
            "<Object><NotTheKeyNode>sample1.txt</NotTheKeyNode></Object>" +
            "<Object><Key>sample2.txt</Key></Object>" +
            "</Delete>";
}
