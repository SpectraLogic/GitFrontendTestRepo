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
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRuleType;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

public final class ModifyAzureTargetRequestHandler_Test 
{
    @Test
    public void testModifyAzureTargetQuiescedStateOnlyAllowedForValidStateTransitions()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final AzureTarget partition = mockDaoDriver.createAzureTarget( "testtp" );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.AZURE_TARGET + "/" + partition.getId() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Quiesced", "NO" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.AZURE_TARGET + "/" + partition.getId() )
                    .addParameter( 
                            ReplicationTarget.QUIESCED.toLowerCase(), 
                            Quiesced.NO.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Quiesced", "NO" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.AZURE_TARGET + "/" + partition.getId() )
                    .addParameter( 
                            ReplicationTarget.QUIESCED.toLowerCase(), 
                            Quiesced.YES.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.AZURE_TARGET + "/" + partition.getId() )
                    .addParameter( 
                            ReplicationTarget.QUIESCED.toLowerCase(), 
                            Quiesced.PENDING.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Quiesced", "PENDING" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.AZURE_TARGET + "/" + partition.getId() )
                    .addParameter( 
                            ReplicationTarget.QUIESCED.toLowerCase(), 
                            Quiesced.YES.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.AZURE_TARGET + "/" + partition.getId() )
                    .addParameter( 
                            ReplicationTarget.QUIESCED.toLowerCase(), 
                            Quiesced.NO.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Quiesced", "NO" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.AZURE_TARGET + "/" + partition.getId() )
                    .addParameter( 
                            AzureTarget.ACCOUNT_NAME, 
                            "id" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Quiesced", "NO" );

        assertEquals(3,  support.getTargetInterfaceBtih().getTotalCallCount(), "Shoulda delegated to data planner.");
    }
    
    
    @Test
    public void testModifyAutoVerificationFrequencyPerformsValidations()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final AzureTarget partition = mockDaoDriver.createAzureTarget( "testtp" );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.AZURE_TARGET + "/" + partition.getId() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/AutoVerifyFrequencyInDays", "" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.AZURE_TARGET + "/" + partition.getId() ).addParameter( 
                        PublicCloudReplicationTarget.AUTO_VERIFY_FREQUENCY_IN_DAYS,
                        "0" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.AZURE_TARGET + "/" + partition.getId() ).addParameter( 
                        PublicCloudReplicationTarget.AUTO_VERIFY_FREQUENCY_IN_DAYS,
                        "365" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/AutoVerifyFrequencyInDays", "365" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.AZURE_TARGET + "/" + partition.getId() ).addParameter( 
                        PublicCloudReplicationTarget.AUTO_VERIFY_FREQUENCY_IN_DAYS,
                        "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/AutoVerifyFrequencyInDays", "" );
    }
    
    
    @Test
    public void testModifyImplicitBucketMappingToCloudNotAllowedIfBreakageWouldOccur()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final AzureTarget target1 = mockDaoDriver.createAzureTarget( "t1" );
        final AzureTarget target2 = mockDaoDriver.createAzureTarget( "t2" );
        final AzureTarget target3 = mockDaoDriver.createAzureTarget( "t3" );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.AZURE_TARGET + "/" + target1.getId() )
            .addParameter( PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX, "abc" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.AZURE_TARGET + "/" + target2.getId() )
            .addParameter( PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX, "abc" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.AZURE_TARGET + "/" + target3.getId() )
            .addParameter( PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX, "abc" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        final DataPolicy dp1 = mockDaoDriver.createDataPolicy( "dp1" );
        mockDaoDriver.createAzureDataReplicationRule(
                dp1.getId(), DataReplicationRuleType.values()[ 0 ], target1.getId() );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "dp2" );
        mockDaoDriver.createAzureDataReplicationRule(
                dp2.getId(), DataReplicationRuleType.values()[ 0 ], target2.getId() );
        final DataPolicy dp3 = mockDaoDriver.createDataPolicy( "dp3" );
        mockDaoDriver.createAzureDataReplicationRule(
                dp3.getId(), DataReplicationRuleType.values()[ 0 ], target3.getId() );
        final Bucket b0 = mockDaoDriver.createBucket( null, dp1.getId(), "b0" );
        final Bucket b1 = mockDaoDriver.createBucket( null, dp1.getId(), "b1" );
        mockDaoDriver.createBucket( null, dp2.getId(), "b2" );
        final Bucket b3 = mockDaoDriver.createBucket( null, dp3.getId(), "b3" );
        mockDaoDriver.createAzureTargetBucketName( b0.getId(), target1.getId(), "b0" );
        mockDaoDriver.createAzureTargetBucketName( b0.getId(), target2.getId(), "b0" );
        mockDaoDriver.createAzureTargetBucketName( b1.getId(), target2.getId(), "b1" );
        mockDaoDriver.createAzureTargetBucketName( b0.getId(), target3.getId(), "b0" );
        mockDaoDriver.createAzureTargetBucketName( b1.getId(), target3.getId(), "b1" );
        mockDaoDriver.createAzureTargetBucketName( b3.getId(), target3.getId(), "b3" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.AZURE_TARGET + "/" + target1.getId() )
            .addParameter( PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX, "abc" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.AZURE_TARGET + "/" + target1.getId() )
            .addParameter( PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX, "ab" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.AZURE_TARGET + "/" + target2.getId() )
            .addParameter( PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX, "ab" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.AZURE_TARGET + "/" + target3.getId() )
            .addParameter( PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX, "ab" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }
}
