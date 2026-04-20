/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import java.util.List;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.exception.ExceptionUtil;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.service.tape.TapeFailureService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.HttpResponseContentVerifier;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;

public final class GetTapesRequestHandler_Test 
{
    @Test
    public void testtestGetTapesReturnsTapes()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final List< Tape > tapes = CollectionFactory.toList(
                mockDaoDriver.createTape( TapeState.NORMAL ),
                mockDaoDriver.createTape( TapeState.FOREIGN ) );
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "tdsn", tapes.get( 0 ).getId() );
        final TapeFailure failure = BeanFactory.newBean(TapeFailure.class)
                .setErrorMessage("OOPSIES")
                .setTapeId(tapes.get(0).getId())
                .setTapeDriveId(drive.getId())
                .setType(TapeFailureType.values()[ 0 ]);
        support.getDatabaseSupport().getServiceManager().getService( TapeFailureService.class ).create(failure);

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/tape" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "OOPSIES" );

        new HttpResponseContentVerifier( driver )
                .verifyTapeNode( "/Data/Tape[1]/", tapes.get( 0 ) )
                .verifyTapeNode( "/Data/Tape[2]/", tapes.get( 1 ) );
    }
    
    
    @Test
    public void testtestGetTapesReturnsTapesSorted() throws InterruptedException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final List< Tape > tapes = CollectionFactory.toList( mockDaoDriver.createTape( TapeState.NORMAL )
                                                                          .setAvailableRawCapacity( 2L ),
                mockDaoDriver.createTape( TapeState.NORMAL )
                             .setAvailableRawCapacity( 4L ) );
        mockDaoDriver.updateBean( tapes.get( 0 ), Tape.AVAILABLE_RAW_CAPACITY );
        mockDaoDriver.updateBean( tapes.get( 1 ), Tape.AVAILABLE_RAW_CAPACITY );

        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "tdsn", tapes.get( 0 )
                                                          .getId() );
        final TapeFailure failure = BeanFactory.newBean(TapeFailure.class)
                .setErrorMessage("OOPSIES")
                .setTapeId(tapes.get(0).getId())
                .setTapeDriveId(drive.getId())
                .setType(TapeFailureType.values()[ 0 ]);
        support.getDatabaseSupport()
               .getServiceManager()
               .getService( TapeFailureService.class )
               .create( failure );
        
        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.GET, "_rest_/tape" ).addParameter( RequestParameterType.SORT_BY.toString(),
                        "availableRawCapacity" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "OOPSIES" );
        
        new HttpResponseContentVerifier( driver ).verifyTapeNode( "/Data/Tape[1]/", tapes.get( 0 ) )
                                                 .verifyTapeNode( "/Data/Tape[2]/", tapes.get( 1 ) );
    }
    
    
    @Test
    public void testtestGetTapesReturnsTapesSortedReversed() throws InterruptedException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final List< Tape > tapes = CollectionFactory.toList( mockDaoDriver.createTape( TapeState.NORMAL )
                                                                          .setBarCode( "One" ),
                mockDaoDriver.createTape( TapeState.NORMAL )
                             .setBarCode( "Two" ) );
        mockDaoDriver.updateBean( tapes.get( 0 ), Tape.BAR_CODE );
        mockDaoDriver.updateBean( tapes.get( 1 ), Tape.BAR_CODE );

        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "tdsn", tapes.get( 0 )
                                                          .getId() );
        final TapeFailure failure = BeanFactory.newBean(TapeFailure.class)
                .setErrorMessage("OOPSIES")
                .setTapeId(tapes.get(0).getId())
                .setTapeDriveId(drive.getId())
                .setType(TapeFailureType.values()[ 0 ]);
        support.getDatabaseSupport()
               .getServiceManager()
               .getService( TapeFailureService.class )
               .create( failure );
        
        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.GET, "_rest_/tape" ).addParameter( RequestParameterType.SORT_BY.toString(),
                        "-barCode" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "OOPSIES" );
        
        new HttpResponseContentVerifier( driver ).verifyTapeNode( "/Data/Tape[1]/", tapes.get( 1 ) )
                                                 .verifyTapeNode( "/Data/Tape[2]/", tapes.get( 0 ) );
    }
    
    
    @Test
    public void testtestGetTapesFailsWithBadSort() throws InterruptedException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockHttpRequestDriver driver =
                new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                        RequestType.GET, "_rest_/tape" ).addParameter( RequestParameterType.SORT_BY.toString(),
                        "badSortColumn" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 500 );
    }
}
