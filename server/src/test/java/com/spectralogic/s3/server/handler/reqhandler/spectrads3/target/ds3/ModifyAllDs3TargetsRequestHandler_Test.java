/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.ds3;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

public final class ModifyAllDs3TargetsRequestHandler_Test 
{
    @Test
    public void testModifyAllDs3TargetsQuiescedStateNoModifiesAllDs3Targets()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Ds3Target tp1 = mockDaoDriver.createDs3Target( "testTp1" );
        final Ds3Target tp2 = mockDaoDriver.createDs3Target( "testTp2" );
        mockDaoDriver.updateBean( tp1.setQuiesced( Quiesced.YES ), ReplicationTarget.QUIESCED );
        mockDaoDriver.updateBean( tp2.setQuiesced( Quiesced.YES ), ReplicationTarget.QUIESCED );
        
        final MockHttpRequestDriver putDriver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.DS3_TARGET.toString() ).addParameter( 
                        ReplicationTarget.QUIESCED.toLowerCase(), 
                        Quiesced.NO.toString() );
        putDriver.run();
        putDriver.assertHttpResponseCodeEquals( 204 );
        assertEquals(Quiesced.NO, mockDaoDriver.attain( Ds3Target.class, tp1.getId() ).getQuiesced(), "Ds3Target tp1 should have been un-quiesced.");
        assertEquals(Quiesced.NO, mockDaoDriver.attain( Ds3Target.class, tp2.getId() ).getQuiesced(), "Ds3Target tp2 should have been un-quiesced.");
    }
    
    
    @Test
    public void testModifyAllDs3TargetsQuiescedStateNoModifiesApplicableDs3Targets()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Ds3Target tp1 = mockDaoDriver.createDs3Target( "testTp1" );
        mockDaoDriver.createDs3Target( "testTp2" );
        mockDaoDriver.updateBean( tp1.setQuiesced( Quiesced.YES ), ReplicationTarget.QUIESCED );
        
        final MockHttpRequestDriver putDriver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.DS3_TARGET.toString() ).addParameter( 
                        ReplicationTarget.QUIESCED.toLowerCase(), 
                        Quiesced.NO.toString() );
        putDriver.run();
        putDriver.assertHttpResponseCodeEquals( 204 );

        assertEquals(Quiesced.NO, mockDaoDriver.attain( Ds3Target.class, tp1.getId() ).getQuiesced(), "Ds3Target tp1 should have been un-quiesced.");
        assertEquals(Quiesced.NO, mockDaoDriver.attain( Ds3Target.class, tp1.getId() ).getQuiesced(), "Ds3Target tp2 should have been un-quiesced.");
    }
    
    
    @Test
    public void testModifyAllDs3TargetsQuiescedStateYesNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Ds3Target tp1 = mockDaoDriver.createDs3Target( "testTp1" );
        mockDaoDriver.createDs3Target( "testTp2" );
        
        final MockHttpRequestDriver putDriver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.DS3_TARGET.toString() ).addParameter( 
                        ReplicationTarget.QUIESCED.toLowerCase(), 
                        Quiesced.YES.toString() );
        putDriver.run();
        putDriver.assertHttpResponseCodeEquals( 409 );

        assertEquals(Quiesced.NO, mockDaoDriver.attain( Ds3Target.class, tp1.getId() ).getQuiesced(), "Ds3Target tp1 should have been un-quiesced.");
        assertEquals(Quiesced.NO, mockDaoDriver.attain( Ds3Target.class, tp1.getId() ).getQuiesced(), "Ds3Target tp2 should have been un-quiesced.");
    }
}
