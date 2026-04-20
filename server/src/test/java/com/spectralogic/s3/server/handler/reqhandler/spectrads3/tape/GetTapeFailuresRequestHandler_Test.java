/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.RequestType;

public final class GetTapeFailuresRequestHandler_Test 
{
    @Test
    public void testtestRequestHandlerReturnsCorrectFilteredResponseForGetTapeFailures()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        final Tape tape3 = mockDaoDriver.createTape();
        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( null, "a" );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( null, "b" );
        
        support.getDatabaseSupport().getDataManager().createBean( BeanFactory.newBean( TapeFailure.class )
                .setErrorMessage( "AAA" )
                .setTapeDriveId( drive1.getId() )
                .setTapeId( tape1.getId() )
                .setType( TapeFailureType.values()[ 0 ] ) );
        support.getDatabaseSupport().getDataManager().createBean( BeanFactory.newBean( TapeFailure.class )
                .setErrorMessage( "BBB" )
                .setTapeDriveId( drive2.getId() )
                .setTapeId( tape1.getId() )
                .setType( TapeFailureType.values()[ 0 ] ) );
        support.getDatabaseSupport().getDataManager().createBean( BeanFactory.newBean( TapeFailure.class )
                .setErrorMessage( "CCC" )
                .setTapeDriveId( drive1.getId() )
                .setTapeId( tape2.getId() )
                .setType( TapeFailureType.values()[ 0 ] ) );
        support.getDatabaseSupport().getDataManager().createBean( BeanFactory.newBean( TapeFailure.class )
                .setErrorMessage( "DDD" )
                .setTapeDriveId( drive1.getId() )
                .setTapeId( tape3.getId() )
                .setType( TapeFailureType.values()[ 0 ] ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "_rest_/tape_failure" )
                    .addParameter( "tapeId", tape1.getId().toString() )
                    .addParameter( "tapeDriveId", drive1.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "AAA" );
        driver.assertResponseToClientDoesNotContain( "BBB" );
        driver.assertResponseToClientDoesNotContain( "CCC" );
        driver.assertResponseToClientDoesNotContain( "DDD" );
    }
}
