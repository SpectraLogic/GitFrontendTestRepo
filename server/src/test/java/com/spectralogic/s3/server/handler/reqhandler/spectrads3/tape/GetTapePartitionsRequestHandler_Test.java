/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import java.util.List;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.HttpResponseContentVerifier;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;

public final class GetTapePartitionsRequestHandler_Test 
{
    @Test
    public void testtestGetTapePartitionsWithoutFullDetailsReturnsTapePartitions()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapePartition partition = mockDaoDriver.createTapePartition( null, "tape partition 1" );
        final List< TapePartition > partitions = CollectionFactory.toList(
                partition,
                mockDaoDriver.createTapePartition( null, "tape partition 2" ) );
        mockDaoDriver.createTape( partition.getId(), TapeState.NORMAL, TapeType.TS_JC );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/tape_partition" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        new HttpResponseContentVerifier( driver )
                .verifyTapePartitionNode( "/Data/TapePartition[1]/", partitions.get( 0 ) )
                .verifyTapePartitionNode( "/Data/TapePartition[2]/", partitions.get( 1 ) );
        driver.assertResponseToClientDoesNotContain( TapeType.TS_JC.toString() );
    }
}
