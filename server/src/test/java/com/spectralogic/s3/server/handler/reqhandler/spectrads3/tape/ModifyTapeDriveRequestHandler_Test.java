package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.util.http.RequestType;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ModifyTapeDriveRequestHandler_Test 
{
    @Test
    public void testtestModifyTapeDriveQuiescedStateOnlyAllowedForValidStateTransitions()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "td1");

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_drive/" + drive.getId() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Quiesced", "NO" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_drive/" + drive.getId() )
                    .addParameter( 
                            TapeDrive.QUIESCED.toLowerCase(), 
                            Quiesced.NO.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Quiesced", "NO" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_drive/" + drive.getId() )
                    .addParameter( 
                            TapeDrive.QUIESCED.toLowerCase(), 
                            Quiesced.YES.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_drive/" + drive.getId() )
                    .addParameter( 
                            TapeDrive.QUIESCED.toLowerCase(), 
                            Quiesced.PENDING.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Quiesced", "PENDING" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_drive/" + drive.getId() )
                    .addParameter( 
                            TapeDrive.QUIESCED.toLowerCase(), 
                            Quiesced.YES.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_drive/" + drive.getId() )
                    .addParameter( 
                            TapeDrive.QUIESCED.toLowerCase(), 
                            Quiesced.NO.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Quiesced", "NO" );
    }
}
