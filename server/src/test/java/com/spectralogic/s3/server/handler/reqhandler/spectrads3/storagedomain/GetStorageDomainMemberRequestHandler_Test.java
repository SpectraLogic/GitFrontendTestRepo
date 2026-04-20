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
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

public final class GetStorageDomainMemberRequestHandler_Test 
{
    @Test
    public void testGetDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( PoolType.values()[ 0 ], "p1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        final StorageDomainMember member = mockDaoDriver.addPoolPartitionToStorageDomain(
                storageDomain.getId(), partition.getId() );
        final StorageDomainMember member2 = mockDaoDriver.addPoolPartitionToStorageDomain(
                storageDomain.getId(), 
                mockDaoDriver.createPoolPartition( PoolType.values()[ 0 ], "p2" ).getId() );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.GET, 
                "_rest_/" + RestDomainType.STORAGE_DOMAIN_MEMBER + "/" + member.getId() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( member.getId().toString() );
        driver.assertResponseToClientDoesNotContain( member2.getId().toString() );
    }
}
