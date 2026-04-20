/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.FeatureKeyType;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveState;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.domain.target.TargetState;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.http.RequestType;

public final class VerifySafeToCreatePutJobRequestHandler_Test 
{
    @Test
    public void testVerifySafeToCreatePutJobValidatesForDs3Targets()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, null );
        mockDaoDriver.createTapeDrive( tp1.getId(), null );
        mockDaoDriver.updateBean( tp1.setState( TapePartitionState.OFFLINE ), TapePartition.STATE );
        final TapePartition tp2 = mockDaoDriver.createTapePartition( null, null );
        mockDaoDriver.createTapeDrive( tp2.getId(), null );
        mockDaoDriver.updateBean( tp2.setQuiesced( Quiesced.PENDING ), TapePartition.QUIESCED );
        final TapePartition tp3a = mockDaoDriver.createTapePartition( null, null );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTapeDrive( tp3a.getId(), null ).setState( TapeDriveState.ERROR ), 
                TapeDrive.STATE );
        final TapePartition tp3b = mockDaoDriver.createTapePartition( null, null );
        final TapePartition tp4 = mockDaoDriver.createTapePartition( null, null );
        mockDaoDriver.createTapeDrive( tp4.getId(), null );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), tp1.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), tp1.getId(), TapeType.LTO6 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), tp2.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), tp3a.getId(), TapeType.LTO6 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd1.getId(), tp3b.getId(), TapeType.LTO6 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tp4.getId(), TapeType.LTO7 );
        mockDaoDriver.addTapePartitionToStorageDomain( sd2.getId(), tp2.getId(), TapeType.LTO7 );
        
        final PoolPartition pp1 = mockDaoDriver.createPoolPartition( PoolType.values()[ 0 ], "pp1" );
        final PoolPartition pp2 = mockDaoDriver.createPoolPartition( PoolType.values()[ 0 ], "pp2" );
        final PoolPartition pp3 = mockDaoDriver.createPoolPartition( PoolType.values()[ 0 ], "pp3" );
        final PoolPartition pp4 = mockDaoDriver.createPoolPartition( PoolType.values()[ 0 ], "pp4" );
        final StorageDomain sd3 = mockDaoDriver.createStorageDomain( "sd3" );
        final StorageDomain sd4 = mockDaoDriver.createStorageDomain( "sd4" );
        mockDaoDriver.createPool( pp1.getId(), PoolState.LOST );
        mockDaoDriver.createPool( pp2.getId(), PoolState.FOREIGN );
        final Pool p3 = mockDaoDriver.createPool( pp3.getId(), PoolState.NORMAL );
        mockDaoDriver.updateBean( p3.setQuiesced( Quiesced.YES ), Pool.QUIESCED );
        mockDaoDriver.createPool( pp4.getId(), PoolState.NORMAL );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd3.getId(), pp1.getId() );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd3.getId(), pp2.getId() );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd3.getId(), pp3.getId() );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd4.getId(), pp3.getId() );
        mockDaoDriver.addPoolPartitionToStorageDomain( sd4.getId(), pp4.getId() );
        
        final Ds3Target t1 = mockDaoDriver.createDs3Target( "t1" );
        final Ds3Target t2 = mockDaoDriver.createDs3Target( "t2" );
        mockDaoDriver.updateBean( t2.setState( TargetState.OFFLINE ), ReplicationTarget.STATE );
        final Ds3Target t3 = mockDaoDriver.createDs3Target( "t3" );
        mockDaoDriver.updateBean( t3.setQuiesced( Quiesced.PENDING ), ReplicationTarget.QUIESCED );

        final Set< DataPolicy > goodDataPolicies = new HashSet<>();
        final Set< DataPolicy > badDataPolicies = new HashSet<>();
        final DataPolicy dp1 = mockDaoDriver.createDataPolicy( "dp1" );
        mockDaoDriver.createDataPersistenceRule(
                dp1.getId(), DataPersistenceRuleType.RETIRED, sd1.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dp1.getId(), DataPersistenceRuleType.TEMPORARY, sd2.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dp1.getId(), DataPersistenceRuleType.PERMANENT, sd4.getId() );
        goodDataPolicies.add( dp1 );
        
        final DataPolicy dp2 = mockDaoDriver.createDataPolicy( "dp2" );
        mockDaoDriver.createDataPersistenceRule(
                dp2.getId(), DataPersistenceRuleType.TEMPORARY, sd1.getId() );
        goodDataPolicies.add( dp2 );
        
        final DataPolicy dp3 = mockDaoDriver.createDataPolicy( "dp3" );
        mockDaoDriver.createDataPersistenceRule(
                dp3.getId(), DataPersistenceRuleType.RETIRED, sd1.getId() );
        goodDataPolicies.add( dp3 );
        
        final DataPolicy dp4 = mockDaoDriver.createDataPolicy( "dp4" );
        mockDaoDriver.createDataPersistenceRule(
                dp4.getId(), DataPersistenceRuleType.PERMANENT, sd3.getId() );
        goodDataPolicies.add( dp4 );
        
        final DataPolicy dp5 = mockDaoDriver.createDataPolicy( "dp5" );
        mockDaoDriver.createDataPersistenceRule(
                dp5.getId(), DataPersistenceRuleType.PERMANENT, sd3.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dp5.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );
        goodDataPolicies.add( dp5 );
        
        final DataPolicy dp6 = mockDaoDriver.createDataPolicy( "dp6" );
        mockDaoDriver.createDataPersistenceRule(
                dp6.getId(), DataPersistenceRuleType.PERMANENT, sd3.getId() );
        mockDaoDriver.updateBean( 
                dp6.setAlwaysForcePutJobCreation( true ),
                DataPolicy.ALWAYS_FORCE_PUT_JOB_CREATION );
        goodDataPolicies.add( dp6 );
        
        final DataPolicy dp7 = mockDaoDriver.createDataPolicy( "dp7" );
        mockDaoDriver.createDs3DataReplicationRule(
                dp7.getId(),
                DataReplicationRuleType.PERMANENT, 
                t1.getId() );
        goodDataPolicies.add( dp7 );
        
        final DataPolicy dp8 = mockDaoDriver.createDataPolicy( "dp8" );
        mockDaoDriver.createDs3DataReplicationRule(
                dp8.getId(),
                DataReplicationRuleType.PERMANENT, 
                t2.getId() );
        badDataPolicies.add( dp8 );
        
        final DataPolicy dp9 = mockDaoDriver.createDataPolicy( "dp9" );
        mockDaoDriver.createDs3DataReplicationRule(
                dp9.getId(),
                DataReplicationRuleType.PERMANENT, 
                t3.getId() );
        badDataPolicies.add( dp9 );
        
        for ( final DataPolicy dp : goodDataPolicies )
        {
            final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "b-" + dp.getName() );
            final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                    support,
                    true,
                    new MockInternalRequestAuthorizationStrategy(),
                    RequestType.PUT, 
                    "_rest_/bucket/" + bucket.getName() );
            driver.addParameter(
                    RequestParameterType.OPERATION.toString(),
                    RestOperationType.VERIFY_SAFE_TO_START_BULK_PUT.toString() );
            driver.run();
            driver.assertHttpResponseCodeEquals( 200 );
        }
        
        for ( final DataPolicy dp : badDataPolicies )
        {
            final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "b-" + dp.getName() );
            final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                    support,
                    true,
                    new MockInternalRequestAuthorizationStrategy(),
                    RequestType.PUT, 
                    "_rest_/bucket/" + bucket.getName() );
            driver.addParameter(
                    RequestParameterType.OPERATION.toString(),
                    RestOperationType.VERIFY_SAFE_TO_START_BULK_PUT.toString() );
            driver.run();
            driver.assertHttpResponseCodeEquals( 400 );
        }
    }
    
    
    @Test
    public void testVerifySafeToCreatePutJobValidatesForAzureTargets()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final AzureTarget target1 = mockDaoDriver.createAzureTarget( "t1" );
        final AzureTarget target2 = mockDaoDriver.createAzureTarget( "t2" );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final Bucket bucket = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket1" );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/bucket/" + bucket.getName() ).addParameter(
                        RequestParameterType.OPERATION.toString(),
                        RestOperationType.VERIFY_SAFE_TO_START_BULK_PUT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        mockDaoDriver.createAzureDataReplicationRule( 
                dataPolicy.getId(), DataReplicationRuleType.RETIRED, target1.getId() );
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/bucket/" + bucket.getName() ).addParameter(
                        RequestParameterType.OPERATION.toString(),
                        RestOperationType.VERIFY_SAFE_TO_START_BULK_PUT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        mockDaoDriver.createAzureDataReplicationRule( 
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target2.getId() );
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/bucket/" + bucket.getName() ).addParameter(
                        RequestParameterType.OPERATION.toString(),
                        RestOperationType.VERIFY_SAFE_TO_START_BULK_PUT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 402 );
        
        mockDaoDriver.createFeatureKey( FeatureKeyType.MICROSOFT_AZURE_CLOUD_OUT );
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/bucket/" + bucket.getName() ).addParameter(
                        RequestParameterType.OPERATION.toString(),
                        RestOperationType.VERIFY_SAFE_TO_START_BULK_PUT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }
    
    
    @Test
    public void testVerifySafeToCreatePutJobValidatesForS3Targets()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final S3Target target1 = mockDaoDriver.createS3Target( "t1" );
        final S3Target target2 = mockDaoDriver.createS3Target( "t2" );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final Bucket bucket = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket1" );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/bucket/" + bucket.getName() ).addParameter(
                        RequestParameterType.OPERATION.toString(),
                        RestOperationType.VERIFY_SAFE_TO_START_BULK_PUT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        mockDaoDriver.createS3DataReplicationRule( 
                dataPolicy.getId(), DataReplicationRuleType.RETIRED, target1.getId() );
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/bucket/" + bucket.getName() ).addParameter(
                        RequestParameterType.OPERATION.toString(),
                        RestOperationType.VERIFY_SAFE_TO_START_BULK_PUT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        mockDaoDriver.createS3DataReplicationRule( 
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target2.getId() );
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/bucket/" + bucket.getName() ).addParameter(
                        RequestParameterType.OPERATION.toString(),
                        RestOperationType.VERIFY_SAFE_TO_START_BULK_PUT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 402 );
        
        mockDaoDriver.createFeatureKey( FeatureKeyType.AWS_S3_CLOUD_OUT );
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/bucket/" + bucket.getName() ).addParameter(
                        RequestParameterType.OPERATION.toString(),
                        RestOperationType.VERIFY_SAFE_TO_START_BULK_PUT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }
}
