/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler;



import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public final class ContentTypeHeaderAndRequestPath_Test 
{
    @Test
    public void testContentTypeHeaderIsRespected()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER_INTERNAL )
            .addParameter( NameObservable.NAME, "jason" );
        driver.run();
        driver.assertResponseToClientContains( "<Name>jason</Name>" ); // Shoulda defaulted to XML

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER_INTERNAL )
            .addParameter( NameObservable.NAME, "barry" ).addHeader( 
                        S3HeaderType.CONTENT_TYPE,
                        "text/json" );
        driver.run();
        driver.assertResponseToClientContains( "\"Name\":\"barry\"" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER_INTERNAL )
            .addParameter( NameObservable.NAME, "justin" ).addHeader( 
                        S3HeaderType.CONTENT_TYPE,
                        "application/json" );
        driver.run();
        driver.assertResponseToClientContains( "\"Name\":\"justin\"" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER_INTERNAL )
            .addParameter( NameObservable.NAME, "rob" ).addHeader( 
                        S3HeaderType.CONTENT_TYPE,
                        "text/xml" );
        driver.run();
        driver.assertResponseToClientContains( "<Name>rob</Name>" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER_INTERNAL )
            .addParameter( NameObservable.NAME, "josh" ).addHeader( 
                        S3HeaderType.CONTENT_TYPE,
                        "text/unknown" );
        driver.run();
        driver.assertResponseToClientContains( "<Name>josh</Name>" ); // Shoulda defaulted to XML
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER_INTERNAL.toString().toLowerCase() + ".xml" )
            .addParameter( NameObservable.NAME, "john" ).addHeader( 
                        S3HeaderType.CONTENT_TYPE,
                        "text/json" );
        driver.run();
        driver.assertResponseToClientContains( "<Name>john</Name>" ); // Request path wins over the header
    }
    
    
    @Test
    public void testAcceptHeaderIsRespected()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER_INTERNAL )
            .addParameter( NameObservable.NAME, "jason" );
        driver.run();
        driver.assertResponseToClientContains( "<Name>jason</Name>" ); // Shoulda defaulted to XML

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER_INTERNAL )
            .addParameter( NameObservable.NAME, "barry" ).addHeader( 
                        S3HeaderType.ACCEPT,
                        "text/json" );
        driver.run();
        driver.assertResponseToClientContains( "\"Name\":\"barry\"" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER_INTERNAL )
            .addParameter( NameObservable.NAME, "justin" ).addHeader( 
                        S3HeaderType.ACCEPT,
                        "application/json" );
        driver.run();
        driver.assertResponseToClientContains( "\"Name\":\"justin\"" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER_INTERNAL )
            .addParameter( NameObservable.NAME, "rob" ).addHeader( 
                        S3HeaderType.ACCEPT,
                        "text/xml" );
        driver.run();
        driver.assertResponseToClientContains( "<Name>rob</Name>" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER_INTERNAL )
            .addParameter( NameObservable.NAME, "josh" ).addHeader( 
                        S3HeaderType.ACCEPT,
                        "text/unknown" );
        driver.run();
        driver.assertResponseToClientContains( "<Name>josh</Name>" ); // Shoulda defaulted to XML
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER_INTERNAL.toString().toLowerCase() + ".xml" )
            .addParameter( NameObservable.NAME, "john" ).addHeader( 
                        S3HeaderType.ACCEPT,
                        "text/json" );
        driver.run();
        driver.assertResponseToClientContains( "<Name>john</Name>" ); // Request path wins over the header
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER_INTERNAL )
            .addParameter( NameObservable.NAME, "jack" ).addHeader( 
                        S3HeaderType.ACCEPT,
                        "text/xml" ).addHeader( 
                                S3HeaderType.CONTENT_TYPE,
                                "text/json" );
        driver.run();
        driver.assertResponseToClientContains( "<Name>jack</Name>" ); // Accept header wins over content-len
    }
    
    
    @Test
    public void testRequestPathFormattingIsRespectedWithAndWithoutLowercase()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER_INTERNAL )
            .addParameter( NameObservable.NAME, "jason" );
        driver.run();
        driver.assertResponseToClientContains( "<Name>jason</Name>" ); // Shoulda defaulted to XML

        final String jsonDomainNotLowercase = RestDomainType.USER_INTERNAL.toString() + ".json";
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + jsonDomainNotLowercase )
            .addParameter( NameObservable.NAME, "barry-not-lowercase" );
        driver.run();
        driver.assertResponseToClientContains( "\"Name\":\"barry-not-lowercase\"" );

        final String jsonDomainLowercase = RestDomainType.USER_INTERNAL.toString().toLowerCase() + ".json";
        assertTrue(
                !jsonDomainLowercase.equals( jsonDomainNotLowercase ),
                "Check the domain was lowercased"
                 );
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + jsonDomainLowercase )
            .addParameter( NameObservable.NAME, "barry-with-lowercase" );
        driver.run();
        driver.assertResponseToClientContains( "\"Name\":\"barry-with-lowercase\"" );
        
        final String xmlDomainNotLowercase = RestDomainType.USER_INTERNAL.toString() + ".xml";
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + xmlDomainNotLowercase )
            .addParameter( NameObservable.NAME, "rob-non-lowercase" ).addHeader( 
                        S3HeaderType.CONTENT_TYPE,
                        "text/xml" );
        driver.run();
        driver.assertResponseToClientContains( "<Name>rob-non-lowercase</Name>" );
        
        final String xmlDomainLowercase = RestDomainType.USER_INTERNAL.toString().toLowerCase() + ".xml";
        assertTrue(
                !xmlDomainLowercase.equals( xmlDomainNotLowercase ),
                "Check the domain was lowercased"
                 );
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + xmlDomainLowercase )
            .addParameter( NameObservable.NAME, "rob-to-lowercase" ).addHeader( 
                        S3HeaderType.CONTENT_TYPE,
                        "text/xml" );
        driver.run();
        driver.assertResponseToClientContains( "<Name>rob-to-lowercase</Name>" );
        
    }
}
