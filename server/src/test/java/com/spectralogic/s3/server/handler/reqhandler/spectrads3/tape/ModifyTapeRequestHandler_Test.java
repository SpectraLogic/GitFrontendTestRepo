/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class ModifyTapeRequestHandler_Test 
{
    @Test
    public void testtestModifyTape()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Tape tape = mockDaoDriver.createTape();

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.TAPE + "/" + tape.getId() )
                        .addParameter( Tape.EJECT_LABEL, "foo_bar" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        driver.assertResponseToClientXPathEquals( "/Data/EjectLabel", "foo_bar" );
    }
    
    
    @Test
    public void testtestModifyTapeStateDoesSoWhenAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService service =
                support.getDatabaseSupport().getServiceManager().getService( TapeService.class );
        service.transistState( tape, TapeState.EJECTED );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.TAPE + "/" + tape.getId() )
                        .addParameter( Tape.STATE, "lost" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        driver.assertResponseToClientXPathEquals( "/Data/State", "LOST" );
        assertEquals(TapeState.LOST, service.attain( tape.getId() ).getState(), "Shoulda updated state.");
    }
    
    
    @Test
    public void testtestModifyTapeStateToCurrentStateAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService service =
                support.getDatabaseSupport().getServiceManager().getService( TapeService.class );
        service.transistState( tape, TapeState.EJECTED );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.TAPE + "/" + tape.getId() )
                        .addParameter( Tape.STATE, "ejected" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        driver.assertResponseToClientXPathEquals( "/Data/State", "EJECTED" );
        assertEquals(TapeState.EJECTED, service.attain( tape.getId() ).getState(), "Shoulda updated state.");
    }
    
    
    @Test
    public void testtestModifyTapeStateToIllegalStateNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService service =
                support.getDatabaseSupport().getServiceManager().getService( TapeService.class );
        service.transistState( tape, TapeState.EJECTED );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.TAPE + "/" + tape.getId() )
                        .addParameter( Tape.STATE, "normal" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        assertEquals(TapeState.EJECTED, service.attain( tape.getId() ).getState(), "Should notta updated state.");
    }
    
    
    @Test
    public void testtestModifyTapeStateFromIllegalStateNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService service =
                support.getDatabaseSupport().getServiceManager().getService( TapeService.class );
        service.transistState( tape, TapeState.NORMAL );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.TAPE + "/" + tape.getId() )
                        .addParameter( Tape.STATE, "lost" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        assertEquals(TapeState.NORMAL, service.attain( tape.getId() ).getState(), "Should notta updated state.");
    }
}
