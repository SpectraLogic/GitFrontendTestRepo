/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrainternal.user;

import java.util.List;
import java.util.UUID;


import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public final class DeleteUserRequestHandler_Test
{
    @Test
    public void testDeleteUserDeletesBean()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User userToDelete = mockDaoDriver.createUser( "test_user_to_delete" );
        mockDaoDriver.createUser( "other_user" );
        
        final UUID beanId = userToDelete.getId();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.USER_INTERNAL + "/test_user_to_delete" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        final List< User > users = support
                .getDatabaseSupport()
                .getServiceManager()
                .getRetriever( User.class )
                .retrieveAll()
                .toList();
        assertEquals(
                1,
                users.size(),
                "Shoulda only had one user left." );
        assertNotEquals(
                beanId,
                users.get( 0 ).getId(),
                "The deleted id should notta remained in the database."
                 );
        assertEquals(
                1,
                support.getTargetInterfaceBtih().getTotalCallCount(),
                "Shoulda delegated to target management."
                 );
    }
    
    
    @Test
    public void testDeleteUserReturns404WhenUserDoesNotExist()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        new MockDaoDriver( support.getDatabaseSupport() ).createUser( "other_user" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.USER_INTERNAL + "/test_user_to_delete" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
        
        final int users = support
                .getDatabaseSupport()
                .getServiceManager()
                .getRetriever( User.class )
                .getCount();
        assertEquals( 1,
                users,
                "Shoulda only had one user left." );
    }
}
