/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.azure;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.AzureTargetBucketName;
import com.spectralogic.s3.common.dao.domain.target.TargetReadPreference;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

public final class CreateAzureTargetBucketNameRequestHandler_Test 
{
    @Test
    public void testCreateDoesNotDelegateToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final AzureTarget target = mockDaoDriver.createAzureTarget( "t1" );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.AZURE_TARGET_BUCKET_NAME )
                .addParameter( TargetReadPreference.TARGET_ID, target.getName() )
                .addParameter( TargetReadPreference.BUCKET_ID, bucket.getName() )
                .addParameter( NameObservable.NAME, "customname" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );

        assertEquals("customname", mockDaoDriver.attainOneAndOnly( AzureTargetBucketName.class ).getName(), "Shoulda created custom name record correctly.");
        assertEquals(0,  support.getDataPolicyBtih().getTotalCallCount(), "Should notta invoked data planner rpc resource.");
    }
}
