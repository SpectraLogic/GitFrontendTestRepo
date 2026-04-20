/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.azure;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

public final class ModifyAllAzureTargetsRequestHandler_Test 
{
    @Test
    public void testModifyAllAzureTargetsQuiescedStateNoModifiesAllAzureTargets()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final AzureTarget tp1 = mockDaoDriver.createAzureTarget( "testTp1" );
        final AzureTarget tp2 = mockDaoDriver.createAzureTarget( "testTp2" );
        mockDaoDriver.updateBean( tp1.setQuiesced( Quiesced.YES ), ReplicationTarget.QUIESCED );
        mockDaoDriver.updateBean( tp2.setQuiesced( Quiesced.YES ), ReplicationTarget.QUIESCED );
        
        final MockHttpRequestDriver putDriver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.AZURE_TARGET.toString() ).addParameter( 
                        ReplicationTarget.QUIESCED.toLowerCase(), 
                        Quiesced.NO.toString() );
        putDriver.run();
        putDriver.assertHttpResponseCodeEquals( 204 );
        assertEquals(Quiesced.NO, mockDaoDriver.attain( AzureTarget.class, tp1.getId() ).getQuiesced(), "AzureTarget tp1 should have been un-quiesced.");
        assertEquals(Quiesced.NO, mockDaoDriver.attain( AzureTarget.class, tp2.getId() ).getQuiesced(), "AzureTarget tp2 should have been un-quiesced.");
    }
    
    
    @Test
    public void testModifyAllAzureTargetsQuiescedStateNoModifiesApplicableAzureTargets()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final AzureTarget tp1 = mockDaoDriver.createAzureTarget( "testTp1" );
        mockDaoDriver.createAzureTarget( "testTp2" );
        mockDaoDriver.updateBean( tp1.setQuiesced( Quiesced.YES ), ReplicationTarget.QUIESCED );
        
        final MockHttpRequestDriver putDriver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.AZURE_TARGET.toString() ).addParameter( 
                        ReplicationTarget.QUIESCED.toLowerCase(), 
                        Quiesced.NO.toString() );
        putDriver.run();
        putDriver.assertHttpResponseCodeEquals( 204 );

        assertEquals(Quiesced.NO, mockDaoDriver.attain( AzureTarget.class, tp1.getId() ).getQuiesced(), "AzureTarget tp1 should have been un-quiesced.");
        assertEquals(Quiesced.NO, mockDaoDriver.attain( AzureTarget.class, tp1.getId() ).getQuiesced(), "AzureTarget tp2 should have been un-quiesced.");
    }
    
    
    @Test
    public void testModifyAllAzureTargetsQuiescedStateYesNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final AzureTarget tp1 = mockDaoDriver.createAzureTarget( "testTp1" );
        mockDaoDriver.createAzureTarget( "testTp2" );
        
        final MockHttpRequestDriver putDriver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.AZURE_TARGET.toString() ).addParameter( 
                        ReplicationTarget.QUIESCED.toLowerCase(), 
                        Quiesced.YES.toString() );
        putDriver.run();
        putDriver.assertHttpResponseCodeEquals( 409 );

        assertEquals(Quiesced.NO, mockDaoDriver.attain( AzureTarget.class, tp1.getId() ).getQuiesced(), "AzureTarget tp1 should have been un-quiesced.");
        assertEquals(Quiesced.NO, mockDaoDriver.attain( AzureTarget.class, tp1.getId() ).getQuiesced(), "AzureTarget tp2 should have been un-quiesced.");
    }
}
