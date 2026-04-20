/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.s3;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRuleType;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.target.DataOfflineablePublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

public final class ModifyS3TargetRequestHandler_Test 
{
    @Test
    public void testModifyS3TargetQuiescedStateOnlyAllowedForValidStateTransitions()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Target partition = mockDaoDriver.createS3Target( "testtp" );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.S3_TARGET + "/" + partition.getId() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Quiesced", "NO" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.S3_TARGET + "/" + partition.getId() )
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
                "_rest_/" + RestDomainType.S3_TARGET + "/" + partition.getId() )
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
                "_rest_/" + RestDomainType.S3_TARGET + "/" + partition.getId() )
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
                "_rest_/" + RestDomainType.S3_TARGET + "/" + partition.getId() )
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
                "_rest_/" + RestDomainType.S3_TARGET + "/" + partition.getId() )
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
                "_rest_/" + RestDomainType.S3_TARGET + "/" + partition.getId() )
                    .addParameter( 
                            S3Target.ACCESS_KEY, 
                            "id" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Quiesced", "NO" );
        
        assertEquals(
                3,
                support.getTargetInterfaceBtih().getTotalCallCount(),
                "Shoulda delegated to data planner."
               );
    }
    
    
    @Test
    public void testModifyOfflineDataStagingWindowPerformsValidations()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Target partition = mockDaoDriver.createS3Target( "testtp" );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.S3_TARGET + "/" + partition.getId() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/OfflineDataStagingWindowInTb", "1" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.S3_TARGET + "/" + partition.getId() ).addParameter( 
                        DataOfflineablePublicCloudReplicationTarget.OFFLINE_DATA_STAGING_WINDOW_IN_TB,
                        "0" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.S3_TARGET + "/" + partition.getId() ).addParameter( 
                        DataOfflineablePublicCloudReplicationTarget.OFFLINE_DATA_STAGING_WINDOW_IN_TB,
                        "65" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.S3_TARGET + "/" + partition.getId() ).addParameter( 
                        DataOfflineablePublicCloudReplicationTarget.OFFLINE_DATA_STAGING_WINDOW_IN_TB,
                        "64" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/OfflineDataStagingWindowInTb", "64" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.S3_TARGET + "/" + partition.getId() ).addParameter( 
                        DataOfflineablePublicCloudReplicationTarget.OFFLINE_DATA_STAGING_WINDOW_IN_TB,
                        "1" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/OfflineDataStagingWindowInTb", "1" );
    }
    
    
    @Test
    public void testModifyStagedDataExpirationPerformsValidations()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Target partition = mockDaoDriver.createS3Target( "testtp" );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.S3_TARGET + "/" + partition.getId() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/StagedDataExpirationInDays", "30" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.S3_TARGET + "/" + partition.getId() ).addParameter( 
                        DataOfflineablePublicCloudReplicationTarget.STAGED_DATA_EXPIRATION_IN_DAYS,
                        "0" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.S3_TARGET + "/" + partition.getId() ).addParameter( 
                        DataOfflineablePublicCloudReplicationTarget.STAGED_DATA_EXPIRATION_IN_DAYS,
                        "366" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.S3_TARGET + "/" + partition.getId() ).addParameter( 
                        DataOfflineablePublicCloudReplicationTarget.STAGED_DATA_EXPIRATION_IN_DAYS,
                        "365" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/StagedDataExpirationInDays", "365" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.S3_TARGET + "/" + partition.getId() ).addParameter( 
                        DataOfflineablePublicCloudReplicationTarget.STAGED_DATA_EXPIRATION_IN_DAYS,
                        "1" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/StagedDataExpirationInDays", "1" );
    }
    
    
    @Test
    public void testModifyAutoVerificationFrequencyPerformsValidations()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Target partition = mockDaoDriver.createS3Target( "testtp" );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.S3_TARGET + "/" + partition.getId() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/AutoVerifyFrequencyInDays", "" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.S3_TARGET + "/" + partition.getId() ).addParameter( 
                        PublicCloudReplicationTarget.AUTO_VERIFY_FREQUENCY_IN_DAYS,
                        "0" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.S3_TARGET + "/" + partition.getId() ).addParameter( 
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
                "_rest_/" + RestDomainType.S3_TARGET + "/" + partition.getId() ).addParameter( 
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
        final S3Target target1 = mockDaoDriver.createS3Target( "t1" );
        final S3Target target2 = mockDaoDriver.createS3Target( "t2" );
        final S3Target target3 = mockDaoDriver.createS3Target( "t3" );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.S3_TARGET + "/" + target1.getId() )
            .addParameter( PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX, "abc" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.S3_TARGET + "/" + target2.getId() )
            .addParameter( PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX, "abc" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.S3_TARGET + "/" + target3.getId() )
            .addParameter( PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX, "abc" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        final DataPolicy dp1 = mockDaoDriver.createDataPolicy( "dp1" );
        mockDaoDriver.createS3DataReplicationRule(
                dp1.getId(), DataReplicationRuleType.values()[ 0 ], target1.getId() );
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "dp2" );
        mockDaoDriver.createS3DataReplicationRule(
                dp2.getId(), DataReplicationRuleType.values()[ 0 ], target2.getId() );
        final DataPolicy dp3 = mockDaoDriver.createDataPolicy( "dp3" );
        mockDaoDriver.createS3DataReplicationRule(
                dp3.getId(), DataReplicationRuleType.values()[ 0 ], target3.getId() );
        final Bucket b0 = mockDaoDriver.createBucket( null, dp1.getId(), "b0" );
        final Bucket b1 = mockDaoDriver.createBucket( null, dp1.getId(), "b1" );
        mockDaoDriver.createBucket( null, dp2.getId(), "b2" );
        final Bucket b3 = mockDaoDriver.createBucket( null, dp3.getId(), "b3" );
        mockDaoDriver.createS3TargetBucketName( b0.getId(), target1.getId(), "b0" );
        mockDaoDriver.createS3TargetBucketName( b0.getId(), target2.getId(), "b0" );
        mockDaoDriver.createS3TargetBucketName( b1.getId(), target2.getId(), "b1" );
        mockDaoDriver.createS3TargetBucketName( b0.getId(), target3.getId(), "b0" );
        mockDaoDriver.createS3TargetBucketName( b1.getId(), target3.getId(), "b1" );
        mockDaoDriver.createS3TargetBucketName( b3.getId(), target3.getId(), "b3" );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.S3_TARGET + "/" + target1.getId() )
            .addParameter( PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX, "abc" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.S3_TARGET + "/" + target1.getId() )
            .addParameter( PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX, "ab" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.S3_TARGET + "/" + target2.getId() )
            .addParameter( PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX, "ab" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.S3_TARGET + "/" + target3.getId() )
            .addParameter( PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX, "ab" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }
}
