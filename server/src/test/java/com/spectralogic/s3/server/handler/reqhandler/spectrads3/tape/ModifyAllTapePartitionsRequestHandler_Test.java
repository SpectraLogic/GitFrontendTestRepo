/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.util.http.RequestType;

public final class ModifyAllTapePartitionsRequestHandler_Test 
{
    @Test
    public void testtestModifyAllTapePartitionsQuiescedStateNoModifiesAllTapePartitions()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, "testTp1" );
        final TapePartition tp2 = mockDaoDriver.createTapePartition( null, "testTp2" );
        mockDaoDriver.updateBean( tp1.setQuiesced( Quiesced.YES ), TapePartition.QUIESCED );
        mockDaoDriver.updateBean( tp2.setQuiesced( Quiesced.YES ), TapePartition.QUIESCED );
        
        final MockHttpRequestDriver putDriver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_partition/" ).addParameter( 
                        TapePartition.QUIESCED.toLowerCase(), 
                        Quiesced.NO.toString() );
        putDriver.run();
        putDriver.assertHttpResponseCodeEquals( 204 );
        assertEquals(Quiesced.NO, mockDaoDriver.attain( TapePartition.class, tp1.getId() ).getQuiesced(), "TapePartition tp1 should have been un-quiesced.");
        assertEquals(Quiesced.NO, mockDaoDriver.attain( TapePartition.class, tp2.getId() ).getQuiesced(), "TapePartition tp2 should have been un-quiesced.");
    }
    
    
    @Test
    public void testtestModifyAllTapePartitionsQuiescedStateNoModifiesApplicableTapePartitions()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, "testTp1" );
        mockDaoDriver.createTapePartition( null, "testTp2" );
        mockDaoDriver.updateBean( tp1.setQuiesced( Quiesced.YES ), TapePartition.QUIESCED );
        
        final MockHttpRequestDriver putDriver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_partition/" ).addParameter( 
                        TapePartition.QUIESCED.toLowerCase(), 
                        Quiesced.NO.toString() );
        putDriver.run();
        putDriver.assertHttpResponseCodeEquals( 204 );

        assertEquals(Quiesced.NO, mockDaoDriver.attain( TapePartition.class, tp1.getId() ).getQuiesced(), "TapePartition tp1 should have been un-quiesced.");
        assertEquals(Quiesced.NO, mockDaoDriver.attain( TapePartition.class, tp1.getId() ).getQuiesced(), "TapePartition tp2 should have been un-quiesced.");
    }
    
    
    @Test
    public void testtestModifyAllTapePartitionsQuiescedStateYesNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, "testTp1" );
        mockDaoDriver.createTapePartition( null, "testTp2" );
        
        final MockHttpRequestDriver putDriver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_partition/" ).addParameter( 
                        TapePartition.QUIESCED.toLowerCase(), 
                        Quiesced.YES.toString() );
        putDriver.run();
        putDriver.assertHttpResponseCodeEquals( 409 );

        assertEquals(Quiesced.NO, mockDaoDriver.attain( TapePartition.class, tp1.getId() ).getQuiesced(), "TapePartition tp1 should have been un-quiesced.");
        assertEquals(Quiesced.NO, mockDaoDriver.attain( TapePartition.class, tp1.getId() ).getQuiesced(), "TapePartition tp2 should have been un-quiesced.");
    }
}
