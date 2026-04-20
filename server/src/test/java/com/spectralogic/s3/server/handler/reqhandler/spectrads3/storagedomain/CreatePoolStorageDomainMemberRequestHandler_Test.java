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
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.http.RequestType;

public final class CreatePoolStorageDomainMemberRequestHandler_Test 
{
    @Test
    public void testCreateDelegatesToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( PoolType.values()[ 0 ], "p1" );
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
                        StorageDomainMember.POOL_PARTITION_ID,
                        partition.getId().toString() )
                .addParameter(
                        StorageDomainMember.STORAGE_DOMAIN_ID,
                        storageDomain.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        support.getDatabaseSupport().getServiceManager().getRetriever( StorageDomainMember.class ).attain(
                Require.nothing() );
        assertEquals(
                1,
                support.getDataPolicyBtih().getTotalCallCount(),
                "Shoulda invoked data planner rpc resource."
                );
    }
}
