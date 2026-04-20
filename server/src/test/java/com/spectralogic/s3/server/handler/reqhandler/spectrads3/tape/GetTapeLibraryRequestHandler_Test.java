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

import com.spectralogic.s3.common.dao.domain.tape.TapeLibrary;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.HttpResponseContentVerifier;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;

public final class GetTapeLibraryRequestHandler_Test 
{
    @Test
    public void testtestGetTapeLibraryReturnsTapeLibrary()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final List< TapeLibrary > libraries = CollectionFactory.toList(
                        mockDaoDriver.createLibrary( "test library" ),
                        mockDaoDriver.createLibrary( "test library 2" ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/tape_library/" + libraries.get( 0 ).getId() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        new HttpResponseContentVerifier( driver ).verifyTapeLibrary( "/Data/", libraries.get( 0 ) );
    }
}
