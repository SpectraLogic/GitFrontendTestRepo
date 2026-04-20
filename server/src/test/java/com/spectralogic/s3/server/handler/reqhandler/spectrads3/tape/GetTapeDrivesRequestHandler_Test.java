/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.HttpResponseContentVerifier;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;

public final class GetTapeDrivesRequestHandler_Test 
{
    @Test
    public void testtestGetTapeDriveReturnsTapeDriveInfo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID partitionId = mockDaoDriver
                .createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN )
                .getId();
        final UUID tapeId = mockDaoDriver.createTape().getId();
        final List< TapeDrive > tapeDrives = CollectionFactory.toList(
                mockDaoDriver.createTapeDrive( partitionId, "test tape drive", tapeId ),
                mockDaoDriver.createTapeDrive( partitionId, "test tape drive 2" ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/tape_drive" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        new HttpResponseContentVerifier( driver )
                .verifyTapeDriveNode( "/Data/TapeDrive[1]/", tapeDrives.get( 0 ) )
                .verifyTapeDriveNode( "/Data/TapeDrive[2]/", tapeDrives.get( 1 ) );
    }
}
