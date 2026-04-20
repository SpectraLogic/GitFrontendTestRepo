/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.storagedomain;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.ds3.WritePreferenceLevel;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.http.RequestType;
import static com.spectralogic.s3.common.testfrmwrk.MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME;

public final class CreateTapeStorageDomainMemberRequestHandler_Test 
{
    @Test
    public void testCreateDelegatesToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapePartition partition = mockDaoDriver.createTapePartition( null, "p1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN_MEMBER )
                .addParameter(
                        StorageDomainMember.WRITE_PREFERENCE,
                        WritePreferenceLevel.values()[ 0 ].toString() )
                .addParameter(
                        StorageDomainMember.TAPE_TYPE,
                        TapeType.values()[ 0 ].toString() )
                .addParameter(
                        StorageDomainMember.TAPE_PARTITION_ID,
                        partition.getId().toString() )
                .addParameter(
                        StorageDomainMember.STORAGE_DOMAIN_ID,
                        storageDomain.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        support.getDatabaseSupport().getServiceManager().getRetriever( StorageDomainMember.class ).attain(
                Require.nothing() );
        assertEquals(1,  support.getDataPolicyBtih().getTotalCallCount(), "Shoulda invoked data planner rpc resource.");
    }
    
    
    @Test
    public void testCreateWritePreferenceNotSupportedFails()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapePartition tp = mockDaoDriver.createTapePartition( null, "tpsn" )
                                              .setDriveType( TapeDriveType.LTO7 );
        mockDaoDriver.updateBean( tp.setDriveType( TapeDriveType.LTO7 ), TapePartition.DRIVE_TYPE );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( DEFAULT_STORAGE_DOMAIN_NAME );
        
        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.POST, "_rest_/" + RestDomainType.STORAGE_DOMAIN_MEMBER ).addParameter(
                        StorageDomainMember.WRITE_PREFERENCE, WritePreferenceLevel.LOW.toString() )
                                                                                            .addParameter(
                                                                                                    StorageDomainMember.TAPE_TYPE,
                                                                                                    TapeType.LTO5.toString() )
                                                                                            .addParameter(
                                                                                                    StorageDomainMember.TAPE_PARTITION_ID,
                                                                                                    tp.getId()
                                                                                                      .toString() )
                                                                                            .addParameter(
                                                                                                    StorageDomainMember.STORAGE_DOMAIN_ID,
                                                                                                    sd.getId()
                                                                                                      .toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        assertEquals(0,  support.getDataPolicyBtih()
                .getTotalCallCount(), "Should not of invoked data planner rpc resource.");
    }
    
    
    @Test
    public void testCreateWritePreferenceSupportedWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapePartition tp = mockDaoDriver.createTapePartition( null, "tpsn" )
                                              .setDriveType( TapeDriveType.LTO7 );
        mockDaoDriver.updateBean( tp.setDriveType( TapeDriveType.LTO7 ), TapePartition.DRIVE_TYPE );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( DEFAULT_STORAGE_DOMAIN_NAME );
        
        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.POST, "_rest_/" + RestDomainType.STORAGE_DOMAIN_MEMBER ).addParameter(
                        StorageDomainMember.WRITE_PREFERENCE, WritePreferenceLevel.NEVER_SELECT.toString() )
                                                                                            .addParameter(
                                                                                                    StorageDomainMember.TAPE_TYPE,
                                                                                                    TapeType.LTO5.toString() )
                                                                                            .addParameter(
                                                                                                    StorageDomainMember.TAPE_PARTITION_ID,
                                                                                                    tp.getId()
                                                                                                      .toString() )
                                                                                            .addParameter(
                                                                                                    StorageDomainMember.STORAGE_DOMAIN_ID,
                                                                                                    sd.getId()
                                                                                                      .toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        assertEquals(1,  support.getDataPolicyBtih()
                .getTotalCallCount(), "Should of invoked data planner rpc resource.");
    }
    
    
    @Test
    public void testCreateWritePreferenceSupportedWorksHuh()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapePartition tp = mockDaoDriver.createTapePartition( null, "tpsn" )
                                              .setDriveType( TapeDriveType.LTO7 );
        mockDaoDriver.updateBean( tp.setDriveType( TapeDriveType.LTO7 ), TapePartition.DRIVE_TYPE );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( DEFAULT_STORAGE_DOMAIN_NAME );
        
        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.POST, "_rest_/" + RestDomainType.STORAGE_DOMAIN_MEMBER ).addParameter(
                        StorageDomainMember.WRITE_PREFERENCE, WritePreferenceLevel.NEVER_SELECT.toString() )
                                                                                            .addParameter(
                                                                                                    StorageDomainMember.TAPE_TYPE,
                                                                                                    TapeType
                                                                                                            .LTO6.toString() )
                                                                                            .addParameter(
                                                                                                    StorageDomainMember.TAPE_PARTITION_ID,
                                                                                                    tp.getId()
                                                                                                      .toString() )
                                                                                            .addParameter(
                                                                                                    StorageDomainMember.STORAGE_DOMAIN_ID,
                                                                                                    sd.getId()
                                                                                                      .toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        assertEquals(1,  support.getDataPolicyBtih()
                .getTotalCallCount(), "Should of invoked data planner rpc resource.");
    }
    
    
    @Test
    public void testCreateWritePreferenceNotPossibleFails()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapePartition tp = mockDaoDriver.createTapePartition( null, "tpsn" )
                                              .setDriveType( TapeDriveType.LTO8 );
        mockDaoDriver.updateBean( tp.setDriveType( TapeDriveType.LTO8 ), TapePartition.DRIVE_TYPE );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( DEFAULT_STORAGE_DOMAIN_NAME );
        
        MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.POST, "_rest_/" + RestDomainType.STORAGE_DOMAIN_MEMBER ).addParameter(
                        StorageDomainMember.WRITE_PREFERENCE, WritePreferenceLevel.NEVER_SELECT.toString() )
                                                                                            .addParameter(
                                                                                                    StorageDomainMember.TAPE_TYPE,
                                                                                                    TapeType.LTO5.toString() )
                                                                                            .addParameter(
                                                                                                    StorageDomainMember.TAPE_PARTITION_ID,
                                                                                                    tp.getId()
                                                                                                      .toString() )
                                                                                            .addParameter(
                                                                                                    StorageDomainMember.STORAGE_DOMAIN_ID,
                                                                                                    sd.getId()
                                                                                                      .toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        assertEquals(0,  support.getDataPolicyBtih()
                .getTotalCallCount(), "Should of invoked data planner rpc resource.");

        driver = new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, "_rest_/" + RestDomainType.STORAGE_DOMAIN_MEMBER ).addParameter(
                StorageDomainMember.WRITE_PREFERENCE, WritePreferenceLevel.NEVER_SELECT.toString() )
                                                                                    .addParameter(
                                                                                            StorageDomainMember
                                                                                                    .TAPE_TYPE,
                                                                                            TapeType.LTO6.toString() )
                                                                                    .addParameter(
                                                                                            StorageDomainMember
                                                                                                    .TAPE_PARTITION_ID,
                                                                                            tp.getId()
                                                                                              .toString() )
                                                                                    .addParameter(
                                                                                            StorageDomainMember
                                                                                                    .STORAGE_DOMAIN_ID,
                                                                                            sd.getId()
                                                                                              .toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        assertEquals(0,  support.getDataPolicyBtih()
                .getTotalCallCount(), "Should of invoked data planner rpc resource.");
    }
}
