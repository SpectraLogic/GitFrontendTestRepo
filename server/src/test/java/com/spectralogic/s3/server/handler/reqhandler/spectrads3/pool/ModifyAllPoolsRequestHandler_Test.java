/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.pool;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.util.http.RequestType;

public final class ModifyAllPoolsRequestHandler_Test 
{
    @Test
    public void testModifyAllPoolsQuiescedStateNoModifiesAllPools()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Pool tp1 = mockDaoDriver.createPool();
        final Pool tp2 = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( tp1.setQuiesced( Quiesced.YES ), Pool.QUIESCED );
        mockDaoDriver.updateBean( tp2.setQuiesced( Quiesced.YES ), Pool.QUIESCED );
        
        final MockHttpRequestDriver putDriver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/pool/" ).addParameter( 
                        Pool.QUIESCED.toLowerCase(), 
                        Quiesced.NO.toString() );
        putDriver.run();
        putDriver.assertHttpResponseCodeEquals( 204 );
        assertEquals(
                Quiesced.NO,
                mockDaoDriver.attain( Pool.class, tp1.getId() ).getQuiesced(),
                "Pool tp1 should have been un-quiesced."
                 );
        assertEquals(
                Quiesced.NO,
                mockDaoDriver.attain( Pool.class, tp2.getId() ).getQuiesced(),
                "Pool tp2 should have been un-quiesced."
                 );
    }
    
    
    @Test
    public void testModifyAllPoolsQuiescedStateNoModifiesApplicablePools()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Pool tp1 = mockDaoDriver.createPool();
        mockDaoDriver.createPool();
        mockDaoDriver.updateBean( tp1.setQuiesced( Quiesced.YES ), Pool.QUIESCED );
        
        final MockHttpRequestDriver putDriver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/pool/" ).addParameter( 
                        Pool.QUIESCED.toLowerCase(), 
                        Quiesced.NO.toString() );
        putDriver.run();
        putDriver.assertHttpResponseCodeEquals( 204 );
        
        assertEquals(
                Quiesced.NO,
                mockDaoDriver.attain( Pool.class, tp1.getId() ).getQuiesced(),
                "Pool tp1 should have been un-quiesced."
                 );
        assertEquals(
                Quiesced.NO,
                mockDaoDriver.attain( Pool.class, tp1.getId() ).getQuiesced() ,
                "Pool tp2 should have been un-quiesced."
                );
    }
    
    
    @Test
    public void testModifyAllPoolsQuiescedStateYesNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Pool tp1 = mockDaoDriver.createPool();
        mockDaoDriver.createPool();
        
        final MockHttpRequestDriver putDriver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/pool/" ).addParameter( 
                        Pool.QUIESCED.toLowerCase(), 
                        Quiesced.YES.toString() );
        putDriver.run();
        putDriver.assertHttpResponseCodeEquals( 409 );
        
        assertEquals(
                Quiesced.NO,
                mockDaoDriver.attain( Pool.class, tp1.getId() ).getQuiesced() ,
                "Pool tp1 should have been un-quiesced."
               );
        assertEquals(
                Quiesced.NO,
                mockDaoDriver.attain( Pool.class, tp1.getId() ).getQuiesced(),
                "Pool tp2 should have been un-quiesced."
                 );
    }
}
