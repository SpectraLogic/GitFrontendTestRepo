/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.s3;

import com.spectralogic.s3.common.dao.domain.ds3.S3Region;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.target.DataOfflineablePublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.domain.target.TargetReadPreferenceType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class RegisterS3TargetRequestHandler_Test 
{
    @Test
    public void testCreateDoesDelegateToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.S3_TARGET )
                .addParameter( NameObservable.NAME, "a" )
                .addParameter( S3Target.ACCESS_KEY, "b" )
                .addParameter( S3Target.SECRET_KEY, "c" )
                .addParameter( S3Target.DATA_PATH_END_POINT, "d" )
                .addParameter( 
                        ReplicationTarget.DEFAULT_READ_PREFERENCE, 
                        TargetReadPreferenceType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        mockDaoDriver.attainOneAndOnly( S3Target.class );
        assertEquals(1,  support.getTargetInterfaceBtih().getTotalCallCount(), "Shoulda invoked data planner rpc resource to register new S3 target.");
    }
    
    
    @Test
    public void testCreateValidatesOfflineDataStagingWindow()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.S3_TARGET )
                .addParameter( NameObservable.NAME, "a" )
                .addParameter( S3Target.ACCESS_KEY, "b" )
                .addParameter( S3Target.SECRET_KEY, "c" )
                .addParameter( S3Target.DATA_PATH_END_POINT, "d" )
                .addParameter(
                        DataOfflineablePublicCloudReplicationTarget.OFFLINE_DATA_STAGING_WINDOW_IN_TB,
                        "0" )
                .addParameter( 
                        ReplicationTarget.DEFAULT_READ_PREFERENCE, 
                        TargetReadPreferenceType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        assertEquals(0,  support.getTargetInterfaceBtih().getTotalCallCount(), "Should notta invoked data planner rpc resource to register new S3 target.");

        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.S3_TARGET )
                .addParameter( NameObservable.NAME, "a" )
                .addParameter( S3Target.ACCESS_KEY, "b" )
                .addParameter( S3Target.SECRET_KEY, "c" )
                .addParameter( S3Target.DATA_PATH_END_POINT, "d" )
                .addParameter(
                        DataOfflineablePublicCloudReplicationTarget.OFFLINE_DATA_STAGING_WINDOW_IN_TB,
                        "1" )
                .addParameter( 
                        ReplicationTarget.DEFAULT_READ_PREFERENCE, 
                        TargetReadPreferenceType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        mockDaoDriver.attainOneAndOnly( S3Target.class );
        assertEquals(1,  support.getTargetInterfaceBtih().getTotalCallCount(), "Shoulda invoked data planner rpc resource to register new S3 target.");
    }
    
    
    @Test
    public void testCreateValidatesStagedDataExpiration()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.S3_TARGET )
                .addParameter( NameObservable.NAME, "a" )
                .addParameter( S3Target.ACCESS_KEY, "b" )
                .addParameter( S3Target.SECRET_KEY, "c" )
                .addParameter( S3Target.DATA_PATH_END_POINT, "d" )
                .addParameter(
                        DataOfflineablePublicCloudReplicationTarget.STAGED_DATA_EXPIRATION_IN_DAYS,
                        "0" )
                .addParameter( 
                        ReplicationTarget.DEFAULT_READ_PREFERENCE, 
                        TargetReadPreferenceType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        assertEquals(0,  support.getTargetInterfaceBtih().getTotalCallCount(), "Should notta invoked data planner rpc resource to register new S3 target.");

        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.S3_TARGET )
                .addParameter( NameObservable.NAME, "a" )
                .addParameter( S3Target.ACCESS_KEY, "b" )
                .addParameter( S3Target.SECRET_KEY, "c" )
                .addParameter( S3Target.DATA_PATH_END_POINT, "d" )
                .addParameter(
                        DataOfflineablePublicCloudReplicationTarget.STAGED_DATA_EXPIRATION_IN_DAYS,
                        "1" )
                .addParameter( 
                        ReplicationTarget.DEFAULT_READ_PREFERENCE, 
                        TargetReadPreferenceType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        mockDaoDriver.attainOneAndOnly( S3Target.class );
        assertEquals(1,  support.getTargetInterfaceBtih().getTotalCallCount(), "Shoulda invoked data planner rpc resource to register new S3 target.");
    }
    
    
    @Test
    public void testS3TargetsWithSameRegionAndAccessKeyNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.S3_TARGET )
                .addParameter( NameObservable.NAME, "a" )
                .addParameter( S3Target.ACCESS_KEY, "b" )
                .addParameter( S3Target.SECRET_KEY, "c" )
                .addParameter( S3Target.REGION, S3Region.US_WEST_1.name() )
                .addParameter( S3Target.DATA_PATH_END_POINT, "d" )
                .addParameter( 
                        ReplicationTarget.DEFAULT_READ_PREFERENCE, 
                        TargetReadPreferenceType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals(
                "Shoulda been able to register s3 target.", 201 );
        
        
        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.S3_TARGET )
                .addParameter( NameObservable.NAME, "a" )
                .addParameter( S3Target.ACCESS_KEY, "b" )
                .addParameter( S3Target.SECRET_KEY, "c" )
                .addParameter( S3Target.REGION, S3Region.US_WEST_1.name() )
                .addParameter( 
                        ReplicationTarget.DEFAULT_READ_PREFERENCE, 
                        TargetReadPreferenceType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals(
                "Should notta conflicted on target with non-null endpoint.", 201 );
        
        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.S3_TARGET )
                .addParameter( NameObservable.NAME, "a" )
                .addParameter( S3Target.ACCESS_KEY, "b1" )
                .addParameter( S3Target.SECRET_KEY, "c" )
                .addParameter( S3Target.REGION, S3Region.US_WEST_1.name() )
                .addParameter( 
                        ReplicationTarget.DEFAULT_READ_PREFERENCE, 
                        TargetReadPreferenceType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals(
                "Should notta conflicted since enpoint preempts region.", 201 );
        
        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.S3_TARGET )
                .addParameter( NameObservable.NAME, "a" )
                .addParameter( S3Target.ACCESS_KEY, "b" )
                .addParameter( S3Target.SECRET_KEY, "c" )
                .addParameter( S3Target.REGION, S3Region.US_WEST_2.name() )
                .addParameter( 
                        ReplicationTarget.DEFAULT_READ_PREFERENCE, 
                        TargetReadPreferenceType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals(
                "Should notta conflicted same on access key with different region.", 201 );
                
        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.S3_TARGET )
                .addParameter( NameObservable.NAME, "a" )
                .addParameter( S3Target.ACCESS_KEY, "b" )
                .addParameter( S3Target.SECRET_KEY, "c" )
                .addParameter( S3Target.REGION, S3Region.US_WEST_1.name() )
                .addParameter( S3Target.DATA_PATH_END_POINT, "d1" )
                .addParameter( 
                        ReplicationTarget.DEFAULT_READ_PREFERENCE, 
                        TargetReadPreferenceType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals(
                "Should notta conflicted since enpoint preempts region.", 201 );
        
        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.S3_TARGET )
                .addParameter( NameObservable.NAME, "a1" )
                .addParameter( S3Target.ACCESS_KEY, "b" )
                .addParameter( S3Target.SECRET_KEY, "c1" )
                .addParameter( S3Target.REGION, S3Region.US_WEST_1.name() )
                .addParameter( 
                        ReplicationTarget.DEFAULT_READ_PREFERENCE, 
                        TargetReadPreferenceType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals(
                "Shoulda conflicted on matching access key and matching region.", 409 );
    }
    
    
    @Test
    public void testS3TargetsWithSameEndpointAndAccessKeyAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.S3_TARGET )
                .addParameter( NameObservable.NAME, "a" )
                .addParameter( S3Target.ACCESS_KEY, "b" )
                .addParameter( S3Target.SECRET_KEY, "c" )
                .addParameter( S3Target.REGION, S3Region.US_WEST_1.name() )
                .addParameter( S3Target.DATA_PATH_END_POINT, "d" )
                .addParameter( 
                        ReplicationTarget.DEFAULT_READ_PREFERENCE, 
                        TargetReadPreferenceType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals(
                "Shoulda been able to register s3 target.", 201 );
        
        
        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.S3_TARGET )
                .addParameter( NameObservable.NAME, "a" )
                .addParameter( S3Target.ACCESS_KEY, "b" )
                .addParameter( S3Target.SECRET_KEY, "c" )
                .addParameter( S3Target.REGION, S3Region.US_WEST_1.name() )
                .addParameter( S3Target.DATA_PATH_END_POINT, "d1" )
                .addParameter( 
                        ReplicationTarget.DEFAULT_READ_PREFERENCE, 
                        TargetReadPreferenceType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals(
                "Should notta conflicted since endpoint is different", 201 );
                
        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.S3_TARGET )
                .addParameter( NameObservable.NAME, "a" )
                .addParameter( S3Target.ACCESS_KEY, "b1" )
                .addParameter( S3Target.SECRET_KEY, "c" )
                .addParameter( S3Target.REGION, S3Region.US_WEST_1.name() )
                .addParameter( S3Target.DATA_PATH_END_POINT, "d" )
                .addParameter( 
                        ReplicationTarget.DEFAULT_READ_PREFERENCE, 
                        TargetReadPreferenceType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals(
                "Should notta conflicted since access key is different", 201 );
                
        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.S3_TARGET )
                .addParameter( NameObservable.NAME, "a" )
                .addParameter( S3Target.ACCESS_KEY, "b" )
                .addParameter( S3Target.SECRET_KEY, "c" )
                .addParameter( S3Target.REGION, S3Region.US_WEST_2.name() )
                .addParameter( S3Target.DATA_PATH_END_POINT, "d" )
                .addParameter( 
                        ReplicationTarget.DEFAULT_READ_PREFERENCE, 
                        TargetReadPreferenceType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals(
                "Shoulda conflicted on matching access key and endpoint pair", 201 );
    }
    
    
    @Test
    public void testCreateValidatesAutoVerificationFrequency()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.S3_TARGET )
                .addParameter( NameObservable.NAME, "a" )
                .addParameter( S3Target.ACCESS_KEY, "b" )
                .addParameter( S3Target.SECRET_KEY, "c" )
                .addParameter( S3Target.DATA_PATH_END_POINT, "d" )
                .addParameter(
                        PublicCloudReplicationTarget.AUTO_VERIFY_FREQUENCY_IN_DAYS,
                        "0" )
                .addParameter( 
                        ReplicationTarget.DEFAULT_READ_PREFERENCE, 
                        TargetReadPreferenceType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        assertEquals(0,  support.getTargetInterfaceBtih().getTotalCallCount(), "Should notta invoked data planner rpc resource to register new S3 target.");

        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.S3_TARGET )
                .addParameter( NameObservable.NAME, "a" )
                .addParameter( S3Target.ACCESS_KEY, "b" )
                .addParameter( S3Target.SECRET_KEY, "c" )
                .addParameter( S3Target.DATA_PATH_END_POINT, "d1" )
                .addParameter(
                        PublicCloudReplicationTarget.AUTO_VERIFY_FREQUENCY_IN_DAYS,
                        "1" )
                .addParameter( 
                        ReplicationTarget.DEFAULT_READ_PREFERENCE, 
                        TargetReadPreferenceType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        mockDaoDriver.attainOneAndOnly( S3Target.class );
        assertEquals(1,  support.getTargetInterfaceBtih().getTotalCallCount(), "Shoulda invoked data planner rpc resource to register new S3 target.");
    }
}
