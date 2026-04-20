/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.storagedomain;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.ds3.WritePreferenceLevel;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
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
import static com.spectralogic.s3.common.testfrmwrk.MockDaoDriver.DEFAULT_DATA_POLICY_NAME;
import static com.spectralogic.s3.common.testfrmwrk.MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME;

public final class ModifyStorageDomainMemberRequestHandler_Test 
{
    @Test
    public void testModifyDelegatesToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( PoolType.values()[ 0 ], "p1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        final StorageDomainMember member = mockDaoDriver.addPoolPartitionToStorageDomain(
                storageDomain.getId(), partition.getId() );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN_MEMBER + "/" + member.getId() )
            .addParameter( 
                    StorageDomainMember.WRITE_PREFERENCE,
                    WritePreferenceLevel.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        support.getDatabaseSupport().getServiceManager().getRetriever( StorageDomainMember.class ).attain(
                Require.nothing() );
        final Object actual = support.getDatabaseSupport().getServiceManager().getRetriever(
                StorageDomainMember.class ).attain( Require.nothing() ).getWritePreference();
        assertEquals(WritePreferenceLevel.values()[ 0 ], actual, "Shoulda updated bean.");
        assertEquals(1,  support.getDataPolicyBtih().getTotalCallCount(), "Shoulda invoked data planner rpc resource.");
    }
    
    
    @Test
    public void testModifyWritePreferenceNotSupportedFails()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapePartition tp = mockDaoDriver.createTapePartition( null, "tpsn" )
                                              .setDriveType( TapeDriveType.LTO7 );
        mockDaoDriver.updateBean( tp.setDriveType( TapeDriveType.LTO7 ), TapePartition.DRIVE_TYPE );
        final DataPolicy dp = mockDaoDriver.createDataPolicy( DEFAULT_DATA_POLICY_NAME );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( DEFAULT_STORAGE_DOMAIN_NAME );
        mockDaoDriver.createDataPersistenceRule( dp.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        final StorageDomainMember sdm =
                mockDaoDriver.addTapePartitionToStorageDomain( sd.getId(), tp.getId(), TapeType.LTO5,
                        WritePreferenceLevel.NEVER_SELECT );
        
        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.PUT,
                        "_rest_/" + RestDomainType.STORAGE_DOMAIN_MEMBER + "/" + sdm.getId() ).addParameter(
                        StorageDomainMember.WRITE_PREFERENCE, WritePreferenceLevel.HIGH.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        assertEquals(WritePreferenceLevel.NEVER_SELECT, support.getDatabaseSupport().getServiceManager().getRetriever(
                        StorageDomainMember.class ).attain( Require.nothing() ).getWritePreference(), "Should not of updated bean.");
        assertEquals(0,  support.getDataPolicyBtih().getTotalCallCount(), "Should not of invoked data planner rpc resource.");
    }
}
