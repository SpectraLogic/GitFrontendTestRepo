/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.request.rest;

import java.util.UUID;



import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.testfrmwrk.MockBeansServiceManager;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public final class RestRequestImpl_Test 
{
    @Test
    public void testValueOfNullRequestTypeNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                RestRequestImpl.valueOf( null, "" );
            }
        } );
    }
    

    @Test
    public void testValueOfNullPathNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            @Test
    public void test()
            {
                RestRequestImpl.valueOf( RequestType.values()[ 0 ], null );
            }
        } );
    }
    
    
    @Test
    public void testGetDomainNotAllowedWhenNotRestful()
    {
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            @Test
    public void test()
            {
                RestRequestImpl.valueOf( RequestType.POST, "/user" ).getDomain();
            }
        } );
    }
    
    
    @Test
    public void testGetIdNotAllowedWhenNotRestful()
    {
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            @Test
    public void test()
            {
                RestRequestImpl.valueOf( RequestType.POST, "/user" ).getId(
                        new MockBeansServiceManager().getRetriever( User.class ) );
            }
        } );
    }
    
    
    @Test
    public void testGetRestActionNotAllowedWhenNotRestful()
    {
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                RestRequestImpl.valueOf( RequestType.POST, "/user" ).getAction();
            }
        } );
    }
    
    
    @Test
    public void testRequestsAreCorrectlyRepresentedAsRestRequestImpls()
    {
        assertFalse(RestRequestImpl.valueOf( RequestType.values()[ 0 ], "" ).isValidRestRequest(), "Should notta recognized as RESTful request.");
        assertFalse(RestRequestImpl.valueOf( RequestType.POST, "/user" ).isValidRestRequest(), "Should notta recognized as RESTful request.");
        assertFalse(RestRequestImpl.valueOf( RequestType.POST, "_rest_/user/id/invalid" ).isValidRestRequest(), "Should notta recognized as RESTful request.");
        assertFalse(RestRequestImpl.valueOf( RequestType.HEAD, "_rest_/user/id" ).isValidRestRequest(), "Should notta recognized as RESTful request.");
        assertTrue(RestRequestImpl.valueOf( RequestType.POST, "_rest_/user" ).isValidRestRequest(), "Shoulda recognized as RESTful request.");
        assertTrue(RestRequestImpl.valueOf( RequestType.POST, "/_rest_/user" ).isValidRestRequest(), "Shoulda recognized as RESTful request.");
        assertTrue(RestRequestImpl.valueOf( RequestType.POST, "_rest_/user/" ).isValidRestRequest(), "Shoulda recognized as RESTful request.");
        assertTrue(RestRequestImpl.valueOf( RequestType.POST, "/_rest_/user/" ).isValidRestRequest(), "Shoulda recognized as RESTful request.");
        assertTrue(RestRequestImpl.valueOf( RequestType.POST, "/_rest_/user/" ).isValidRestRequest(), "Shoulda recognized as RESTful request.");
        assertTrue(RestRequestImpl.valueOf( RequestType.POST, "/_rest_/USER/" ).isValidRestRequest(), "Shoulda recognized as RESTful request.");

        final RestRequest restRequestUserInternal1 = RestRequestImpl.valueOf( RequestType.POST, 
                "/_rest_/USER_INTERNAL.json" );
        assertTrue(restRequestUserInternal1.isValidRestRequest(), "Shoulda recognized as RESTful request.");
        assertTrue("USER_INTERNAL".equals( restRequestUserInternal1.getDomain().name() ), "Shoulda come back uppercase with no .json.");
        final RestRequest restRequestUserInternal2 = RestRequestImpl.valueOf( RequestType.POST,
                "/_rest_/USER_INTERNAL/" );
        assertTrue(restRequestUserInternal2.isValidRestRequest(), "Shoulda recognized as RESTful request.");
        assertTrue(restRequestUserInternal1.getDomain().name().equals(
                        restRequestUserInternal2.getDomain().name() ), "Shoulda be the same as restRequestUserInternal1.");

        assertTrue(!RestRequestImpl.valueOf( RequestType.POST, "/_rest_/" ).isValidRestRequest(), "Shoulda NOT be recognized as RESTful request.");
    }
    
    
    @Test
    public void testHandlesCreateRequestsWhenDoneAsGet()
    {
        final RestRequest r = RestRequestImpl.valueOf( RequestType.GET, "/_rest_/user/new" );
        final Object actual2 = r.isValidRestRequest();
        assertEquals( true, actual2, "Shoulda represented request correctly.");
        final Object actual1 = r.getAction();
        assertEquals( RestActionType.CREATE, actual1, "Shoulda represented request correctly.");
        final Object actual = r.getDomain();
        assertEquals( RestDomainType.USER, actual, "Shoulda represented request correctly.");
        TestUtil.assertThrows(
                null,
                S3RestException.class, new BlastContainer()
                {
                    @Test
    public void test()
                    {
                        assertEquals(null,  r.getId(new MockBeansServiceManager().getRetriever(User.class)), "Shoulda represented request correctly.");
                    }
                } );
    }
    
    
    @Test
    public void testHandlesCreateRequestsWhenDoneAsPost()
    {
        final RestRequest r = RestRequestImpl.valueOf( RequestType.POST, "/_rest_/user" );
        final Object actual2 = r.isValidRestRequest();
        assertEquals( true, actual2, "Shoulda represented request correctly.");
        final Object actual1 = r.getAction();
        assertEquals( RestActionType.CREATE, actual1, "Shoulda represented request correctly.");
        final Object actual = r.getDomain();
        assertEquals( RestDomainType.USER, actual, "Shoulda represented request correctly.");
        TestUtil.assertThrows(
                null,
                S3RestException.class, new BlastContainer()
                {
                    @Test
    public void test()
                    {
                        assertEquals(null,  r.getId(new MockBeansServiceManager().getRetriever(User.class)), "Shoulda represented request correctly.");
                    }
                } );
    }
    
    
    @Test
    public void testHandlesListRequests()
    {
        final RestRequest r = RestRequestImpl.valueOf( RequestType.GET, "/_rest_/user/" );
        final Object actual2 = r.isValidRestRequest();
        assertEquals( true, actual2, "Shoulda represented request correctly.");
        final Object actual1 = r.getAction();
        assertEquals( RestActionType.LIST, actual1, "Shoulda represented request correctly.");
        final Object actual = r.getDomain();
        assertEquals( RestDomainType.USER, actual, "Shoulda represented request correctly.");
        TestUtil.assertThrows(
                null,
                S3RestException.class, new BlastContainer()
                {
                    @Test
    public void test()
                    {
                        assertEquals(null,  r.getId(new MockBeansServiceManager().getRetriever(User.class)), "Shoulda represented request correctly.");
                    }
                } );
    }
    
    
    @Test
    public void testHandlesShowRequests()
    {
        final RestRequest r = RestRequestImpl.valueOf( 
                RequestType.GET,
                "/_rest_/user/d255a158-931d-457d-97cb-5edc34494031" );
        final Object actual2 = r.isValidRestRequest();
        assertEquals( true, actual2, "Shoulda represented request correctly.");
        final Object actual1 = r.getAction();
        assertEquals( RestActionType.SHOW, actual1, "Shoulda represented request correctly.");
        final Object actual = r.getDomain();
        assertEquals( RestDomainType.USER, actual, "Shoulda represented request correctly.");
        final Object expected = UUID.fromString( "d255a158-931d-457d-97cb-5edc34494031" );
        assertEquals(expected,  r.getId(new MockBeansServiceManager().getRetriever(User.class)), "Shoulda represented request correctly.");
    }
    
    
    @Test
    public void testHandlesSingletonDeleteRequests()
    {
        assertTrue(RestRequestImpl.valueOf( RequestType.DELETE, "/_rest_/user/" ).isValidRestRequest(), "Shoulda passed validation for a non-singleton resource.");
    }
    
    
    @Test
    public void testHandlesNonSingletonDeleteRequests()
    {
        final RestRequest r = RestRequestImpl.valueOf(
                RequestType.DELETE, 
                "/_rest_/user/d255a158-931d-457d-97cb-5edc34494031" );
        final Object actual2 = r.isValidRestRequest();
        assertEquals( true, actual2, "Shoulda represented request correctly.");
        final Object actual1 = r.getAction();
        assertEquals( RestActionType.DELETE, actual1, "Shoulda represented request correctly.");
        final Object actual = r.getDomain();
        assertEquals( RestDomainType.USER, actual, "Shoulda represented request correctly.");
        final Object expected = UUID.fromString( "d255a158-931d-457d-97cb-5edc34494031" );
        assertEquals(expected,  r.getId(new MockBeansServiceManager().getRetriever(User.class)), "Shoulda represented request correctly.");
    }
    
    
    @Test
    public void testHandlesBulkModifyRequests()
    {
        final RestRequest r = RestRequestImpl.valueOf( 
                RequestType.PUT, 
                "/_rest_/user/" );
        final Object actual2 = r.isValidRestRequest();
        assertEquals( true, actual2, "Shoulda represented request correctly.");
        final Object actual1 = r.getAction();
        assertEquals( RestActionType.BULK_MODIFY, actual1, "Shoulda represented request correctly.");
        final Object actual = r.getDomain();
        assertEquals( RestDomainType.USER, actual, "Shoulda represented request correctly.");
        TestUtil.assertThrows(
                null,
                S3RestException.class, new BlastContainer()
                {
                    @Test
    public void test()
                    {
                        assertEquals(null,  r.getId(new MockBeansServiceManager().getRetriever(User.class)), "Shoulda represented request correctly.");
                    }
                } );
    }
    
    
    @Test
    public void testHandlesNonSingletonModifyRequests()
    {
        final RestRequest r = RestRequestImpl.valueOf( 
                RequestType.PUT, 
                "/_rest_/user/d255a158-931d-457d-97cb-5edc34494031" );
        final Object actual2 = r.isValidRestRequest();
        assertEquals( true, actual2, "Shoulda represented request correctly.");
        final Object actual1 = r.getAction();
        assertEquals( RestActionType.MODIFY, actual1, "Shoulda represented request correctly.");
        final Object actual = r.getDomain();
        assertEquals( RestDomainType.USER, actual, "Shoulda represented request correctly.");
        final Object expected = UUID.fromString( "d255a158-931d-457d-97cb-5edc34494031" );
        assertEquals(expected,  r.getId(new MockBeansServiceManager().getRetriever(User.class)), "Shoulda represented request correctly.");
    }
}
