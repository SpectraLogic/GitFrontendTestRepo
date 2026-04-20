/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.system;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;

import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockAnonymousAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.marshal.XmlMarshaler;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class GetRequestHandlersRequestHandler_Test 
{
    @Test
    public void testGetRequestHandlersRequestHandlerReturnsRequestHandlersWhenShortDetails()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o = mockDaoDriver.createObject( null, "o1", -1 );
        mockDaoDriver.createBlobs( o.getId(), 2, 100 );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockAnonymousAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.REQUEST_HANDLER );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        writeSelfDocumentationToTempDir( "request-handlers.xml", driver.getResponseToClientAsString() );
    }
    
    
    @Test
    public void testGetRequestHandlersRequestHandlerReturnsRequestHandlersWhenFullDetails()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o = mockDaoDriver.createObject( null, "o1", -1 );
        mockDaoDriver.createBlobs( o.getId(), 2, 100 );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockAnonymousAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.REQUEST_HANDLER )
            .addParameter( RequestParameterType.FULL_DETAILS.toString(), "1" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        writeSelfDocumentationToTempDir( "request-handlers-full.xml", driver.getResponseToClientAsString() );
    }
    
    
    @Test
    public void testCreateResponseSelfDocumentationContract()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o = mockDaoDriver.createObject( null, "o1", -1 );
        mockDaoDriver.createBlobs( o.getId(), 2, 100 );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockAnonymousAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.REQUEST_HANDLER_CONTRACT );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        writeSelfDocumentationToTempDir( 
                "request-handlers-contract.xml",
                driver.getResponseToClientAsString() );
    }
    
    
    private void writeSelfDocumentationToTempDir( final String fileName, final String selfDocumentation )
    {
        final File file = 
                new File( System.getProperty( "java.io.tmpdir" ) + Platform.FILE_SEPARATOR + fileName );
        FileWriter out = null;
        try
        {
            out = new FileWriter( file );
                try ( final BufferedWriter bout = new BufferedWriter( out ) )
                {
                bout.write( XmlMarshaler.formatPretty( selfDocumentation )
                        .replace( "</RequestHandler>", "</RequestHandler>" + Platform.NEWLINE )
                        .replace( "</Type>", "</Type>" + Platform.NEWLINE ) );
                bout.flush();
            }
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( ex );
        }
        finally
        {
            if ( null != out )
            {
                try
                {
                    out.close();
                }
                catch ( final IOException ex )
                {
                    throw new RuntimeException( ex );
                }
            }
        }
        
        final URL p4FileUrl = 
                GetRequestHandlersRequestHandler.class.getResource( "/" + fileName );
        try
        {
            if ( null == p4FileUrl )
            {
                throw new RuntimeException( fileName + " is missing." );
            }
            
            final InputStream p4FileIs = 
                    GetRequestHandlersRequestHandler.class.getResourceAsStream( "/" + fileName );
            final String p4FileContents = IOUtils.toString( p4FileIs, Charset.defaultCharset() );
            p4FileIs.close();
    
            assertTrue(
                    getSelfDocsForComparison( selfDocumentation )
                            .equals( getSelfDocsForComparison( p4FileContents ) ),
                    "You need to re-generate " + fileName + " from " + file + "."
                     );
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( "Failed to validate " + p4FileUrl, ex );
        }
    }
    
    
    private String getSelfDocsForComparison( final String selfDocs )
    {
        return selfDocs.replace( Platform.SLASH_N, "" ).replace( Platform.SLASH_R, "" )
                .replace( "&quot;", "\"" ).replace( "&apos;", "'" ).replace( " ", "" );
    }
}
