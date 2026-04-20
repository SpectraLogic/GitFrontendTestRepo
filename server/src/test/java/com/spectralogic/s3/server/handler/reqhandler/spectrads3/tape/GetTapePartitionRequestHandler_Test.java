/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.HttpResponseContentVerifier;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.util.http.RequestType;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class GetTapePartitionRequestHandler_Test 
{
    @Test
    public void testtestGetTapePartitionReturnsTapePartition()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapePartition partition = mockDaoDriver.createTapePartition( null, "tape partition", null );
        mockDaoDriver.createTape(
                mockDaoDriver.createTapePartition( null, "tape partition 2", null ).getId(),
                TapeState.NORMAL,
                TapeType.LTO5 );
        mockDaoDriver.createTape( partition.getId(), TapeState.NORMAL, TapeType.LTO6 );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTapeDrive( partition.getId(), "tdsn" ).setType( TapeDriveType.TS1140 ),
                TapeDrive.TYPE );
        mockDaoDriver.updateBean( 
                mockDaoDriver.createTapeDrive( 
                        mockDaoDriver.createTapePartition( null, "tapp3" ).getId(), "tdsn2" )
                .setType( TapeDriveType.TS1150 ),
                TapeDrive.TYPE );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/tape_partition/" + partition.getId() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( TapeType.LTO6.toString() );
        driver.assertResponseToClientDoesNotContain( TapeType.LTO5.toString() );
        driver.assertResponseToClientDoesNotContain( TapeDriveType.TS1140.toString() );
        driver.assertResponseToClientDoesNotContain( TapeDriveType.TS1150.toString() );

        new HttpResponseContentVerifier( driver ).verifyTapePartitionNode( "/Data/", partition );
    }
}
