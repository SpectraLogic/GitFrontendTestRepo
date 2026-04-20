/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.acl;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAcl;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicyAcl;
import com.spectralogic.s3.common.dao.domain.ds3.Group;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class CreateGlobalBucketAclForGroupRequestHandler_Test 
{
    @Test
    public void testCreateDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.deleteAll( BucketAcl.class );
        final Group group = mockDaoDriver.createGroup( "group1" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.BUCKET_ACL )
                .addParameter(
                        BucketAcl.PERMISSION, 
                        BucketAclPermission.values()[ 0 ].toString() )
                .addParameter(
                        DataPolicyAcl.GROUP_ID,
                        group.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        assertEquals(
                1,
                support.getDatabaseSupport().getServiceManager().getRetriever(
                        BucketAcl.class ).getCount(),
                "Shoulda created data policy ACL."
                 );
    }
}
