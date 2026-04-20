/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;


public final class PagingRequestParametersForListRequestHandlersIntegration_Test 
{
    @Test
    public void testGetWithoutPageOffsetOrLengthReturnsAllResults()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
    
        create6Users( support );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.USER );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "user1" );
        driver.assertResponseToClientContains( "user2" );
        driver.assertResponseToClientContains( "user3" );
        driver.assertResponseToClientContains( "user4" );
        driver.assertResponseToClientContains( "user5" );
        
        final Map< String, String > headers = new HashMap<>();
        driver.assertResponseToClientHasHeaders( headers );
    }
    
    
    @Test
    public void testGetResultCountWithoutResultsWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createUser( "user" );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.USER )
            .addParameter( RequestParameterType.PAGE_LENGTH.toString(), "0" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "user" );
        
        final Map< String, String > headers = new HashMap<>();
        headers.put( S3HeaderType.PAGING_TOTAL_RESULT_COUNT.getHttpHeaderName(), "1" );
        headers.put( S3HeaderType.PAGING_TRUNCATED.getHttpHeaderName(), "1" );
        driver.assertResponseToClientHasHeaders( headers );
    }
    
    
    @Test
    public void testGetWithPageOffsetButNoPageLengthReturnsResultsWithOffset()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
    
        create6Users( support );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.USER )
            .addParameter( RequestParameterType.PAGE_OFFSET.toString(), "2" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "user1" );
        driver.assertResponseToClientDoesNotContain( "user2" );
        driver.assertResponseToClientContains( "user3" );
        driver.assertResponseToClientContains( "user4" );
        driver.assertResponseToClientContains( "user5" );
        
        final Map< String, String > headers = new HashMap<>();
        headers.put( S3HeaderType.PAGING_TOTAL_RESULT_COUNT.getHttpHeaderName(), "5" );
        headers.put( S3HeaderType.PAGING_TRUNCATED.getHttpHeaderName(), "0" );
        driver.assertResponseToClientHasHeaders( headers );
    }
    
    
    @Test
    public void testGetWithPageLengthButNoPageOffsetReturnsResultsWithLengthLimit()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
    
        create6Users( support );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.USER )
            .addParameter( RequestParameterType.PAGE_LENGTH.toString(), "2" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "user1" );
        driver.assertResponseToClientContains( "user2" );
        driver.assertResponseToClientDoesNotContain( "user3" );
        driver.assertResponseToClientDoesNotContain( "user4" );
        driver.assertResponseToClientDoesNotContain( "user5" );
        
        final Map< String, String > headers = new HashMap<>();
        headers.put( S3HeaderType.PAGING_TOTAL_RESULT_COUNT.getHttpHeaderName(), "5" );
        headers.put( S3HeaderType.PAGING_TRUNCATED.getHttpHeaderName(), "3" );
        driver.assertResponseToClientHasHeaders( headers );
    }
    
    
    @Test
    public void testGetWithPageOffsetAndPageLengthReturnsResultsWithOffsetAndLength()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
    
        create6Users( support );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.USER )
            .addParameter( RequestParameterType.PAGE_OFFSET.toString(), "1" )
            .addParameter( RequestParameterType.PAGE_LENGTH.toString(), "2" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "user1" );
        driver.assertResponseToClientContains( "user2" );
        driver.assertResponseToClientContains( "user3" );
        driver.assertResponseToClientDoesNotContain( "user4" );
        driver.assertResponseToClientDoesNotContain( "user5" );
        
        final Map< String, String > headers = new HashMap<>();
        headers.put( S3HeaderType.PAGING_TOTAL_RESULT_COUNT.getHttpHeaderName(), "5" );
        headers.put( S3HeaderType.PAGING_TRUNCATED.getHttpHeaderName(), "2" );
        driver.assertResponseToClientHasHeaders( headers );
    }
    
    
    @Test
    public void testGetWithInvalidPageLengthFails()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
    
        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.GET, "_rest_/" + RestDomainType.USER )
                        .addParameter( RequestParameterType.PAGE_LENGTH.toString(), "1001" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testGetWithPageOffsetAndPageStartMarkerNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        create6Users( support );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.USER )
            .addParameter( RequestParameterType.PAGE_OFFSET.toString(), "2" )
            .addParameter( RequestParameterType.PAGE_START_MARKER.toString(), UUID.randomUUID().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        final Map< String, String > headers = new HashMap<>();
        driver.assertResponseToClientHasHeaders( headers );
    }
    
    
    @Test
    public void testGetWithPageOffsetAndLastPageNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
    
        create6Users( support );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.USER )
            .addParameter( RequestParameterType.PAGE_OFFSET.toString(), "2" )
            .addParameter( RequestParameterType.LAST_PAGE.toString(), "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        final Map< String, String > headers = new HashMap<>();
        driver.assertResponseToClientHasHeaders( headers );
    }
    
    
    @Test
    public void testGetWithPageStartMarkerHonorsStartMarker()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
    
        final List< UUID > ids = create6Users( support );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.USER )
            .addParameter( RequestParameterType.PAGE_START_MARKER.toString(), ids.get( 1 ).toString() );
        driver.run();
        driver.assertResponseToClientDoesNotContain( "user1" );
        driver.assertResponseToClientDoesNotContain( "user2" );
        driver.assertResponseToClientContains( "user3" );
        driver.assertResponseToClientContains( "user4" );
        driver.assertResponseToClientContains( "user5" );
        
        final Map< String, String > headers = new HashMap<>();
        headers.put( S3HeaderType.PAGING_TOTAL_RESULT_COUNT.getHttpHeaderName(), "5" );
        headers.put( S3HeaderType.PAGING_TRUNCATED.getHttpHeaderName(), "0" );
        driver.assertResponseToClientHasHeaders( headers );
    }
    
    
    private List< UUID > create6Users( final MockHttpRequestSupport support )
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final List< UUID > ids = new ArrayList<>();
        for ( int i = 1; i < 6; ++i )
        {
            ids.add( mockDaoDriver.createUser( "user" + i ).getId() );
        }
        return ids;
    }
    
    
    @Test
    public void testGetWithPageStartMarkerAndPageLengthReturnsResultsWithOffsetAndLength()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
    
        final List< UUID > ids = create6Users( support );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.USER )
            .addParameter( RequestParameterType.PAGE_START_MARKER.toString(), ids.get( 0 ).toString() )
            .addParameter( RequestParameterType.PAGE_LENGTH.toString(), "2" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "user1" );
        driver.assertResponseToClientContains( "user2" );
        driver.assertResponseToClientContains( "user3" );
        driver.assertResponseToClientDoesNotContain( "user4" );
        driver.assertResponseToClientDoesNotContain( "user5" );
        
        final Map< String, String > headers = new HashMap<>();
        headers.put( S3HeaderType.PAGING_TOTAL_RESULT_COUNT.getHttpHeaderName(), "5" );
        headers.put( S3HeaderType.PAGING_TRUNCATED.getHttpHeaderName(), "2" );
        driver.assertResponseToClientHasHeaders( headers );
    }
    
    
    @Test
    public void testGetWithPageStartMarkerThatDoesNotExistNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
    
        create6Users( support );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.USER )
            .addParameter( RequestParameterType.PAGE_START_MARKER.toString(), UUID.randomUUID().toString() )
            .addParameter( RequestParameterType.PAGE_LENGTH.toString(), "2" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
        
        final Map< String, String > headers = new HashMap<>();
        driver.assertResponseToClientHasHeaders( headers );
    }
    
    
    @Test
    public void testGetWithLastPageMarkerAndPageLengthReturnsResultsWithOffsetAndLength()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
    
        create6Users( support );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.USER )
            .addParameter( RequestParameterType.LAST_PAGE.toString(), "" )
            .addParameter( RequestParameterType.PAGE_LENGTH.toString(), "2" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "user1" );
        driver.assertResponseToClientDoesNotContain( "user2" );
        driver.assertResponseToClientDoesNotContain( "user3" );
        driver.assertResponseToClientContains( "user4" );
        driver.assertResponseToClientContains( "user5" );
        
        final Map< String, String > headers = new HashMap<>();
        headers.put( S3HeaderType.PAGING_TOTAL_RESULT_COUNT.getHttpHeaderName(), "5" );
        headers.put( S3HeaderType.PAGING_TRUNCATED.getHttpHeaderName(), "0" );
        driver.assertResponseToClientHasHeaders( headers );
    }
    
    
    @Test
    public void testGetWithLastPageMarkerAndNoPageLengthWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        create6Users( support );
        
        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.GET, "_rest_/" + RestDomainType.USER ).addParameter(
                        RequestParameterType.LAST_PAGE.toString(), "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "user1" );
        driver.assertResponseToClientContains( "user2" );
        driver.assertResponseToClientContains( "user3" );
        driver.assertResponseToClientContains( "user4" );
        driver.assertResponseToClientContains( "user5" );
        
        final Map< String, String > headers = new HashMap<>();
        headers.put( S3HeaderType.PAGING_TOTAL_RESULT_COUNT.getHttpHeaderName(), "5" );
        headers.put( S3HeaderType.PAGING_TRUNCATED.getHttpHeaderName(), "0" );
        driver.assertResponseToClientHasHeaders( headers );
    }
}
