/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.azure;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.TargetReadPreferenceType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

public final class RegisterAzureTargetRequestHandler_Test 
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
                "_rest_/" + RestDomainType.AZURE_TARGET )
                .addParameter( NameObservable.NAME, "a" )
                .addParameter( AzureTarget.ACCOUNT_NAME, "b" )
                .addParameter( AzureTarget.ACCOUNT_KEY, "c" )
                .addParameter( 
                        ReplicationTarget.DEFAULT_READ_PREFERENCE, 
                        TargetReadPreferenceType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        mockDaoDriver.attainOneAndOnly( AzureTarget.class );
        assertEquals(1,  support.getTargetInterfaceBtih().getTotalCallCount(), "Shoulda invoked data planner rpc resource to register new AZURE target.");
    }
    
    
    @Test
    public void testCreateWithExplicitEmptyCloudPrefixWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.AZURE_TARGET )
                .addParameter( NameObservable.NAME, "a" )
                .addParameter( AzureTarget.ACCOUNT_NAME, "b" )
                .addParameter( AzureTarget.ACCOUNT_KEY, "c" )
                .addParameter( PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX, "" )
                .addParameter( 
                        ReplicationTarget.DEFAULT_READ_PREFERENCE, 
                        TargetReadPreferenceType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        mockDaoDriver.attainOneAndOnly( AzureTarget.class );
        assertEquals(1,  support.getTargetInterfaceBtih().getTotalCallCount(), "Shoulda invoked data planner rpc resource to register new AZURE target.");
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
                "_rest_/" + RestDomainType.AZURE_TARGET )
                .addParameter( NameObservable.NAME, "a" )
                .addParameter( AzureTarget.ACCOUNT_NAME, "b" )
                .addParameter( AzureTarget.ACCOUNT_KEY, "c" )
                .addParameter(
                        PublicCloudReplicationTarget.AUTO_VERIFY_FREQUENCY_IN_DAYS,
                        "0" )
                .addParameter( 
                        ReplicationTarget.DEFAULT_READ_PREFERENCE, 
                        TargetReadPreferenceType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        assertEquals(0,  support.getTargetInterfaceBtih().getTotalCallCount(), "Should notta invoked data planner rpc resource to register new Azure target.");

        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.AZURE_TARGET )
                .addParameter( NameObservable.NAME, "a" )
                .addParameter( AzureTarget.ACCOUNT_NAME, "b" )
                .addParameter( AzureTarget.ACCOUNT_KEY, "c" )
                .addParameter(
                        PublicCloudReplicationTarget.AUTO_VERIFY_FREQUENCY_IN_DAYS,
                        "1" )
                .addParameter( 
                        ReplicationTarget.DEFAULT_READ_PREFERENCE, 
                        TargetReadPreferenceType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        mockDaoDriver.attainOneAndOnly( AzureTarget.class );
        assertEquals(1,  support.getTargetInterfaceBtih().getTotalCallCount(), "Shoulda invoked data planner rpc resource to register new Azure target.");
    }
}
