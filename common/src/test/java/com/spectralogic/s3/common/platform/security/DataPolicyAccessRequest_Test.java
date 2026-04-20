/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.security;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class DataPolicyAccessRequest_Test 
{
    @Test
    public void testConstructorNullUserIdNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new DataPolicyAccessRequest( null, UUID.randomUUID() );
            }
        } );
    }
    

    @Test
    public void testConstructorNullDataPolicyIdNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new DataPolicyAccessRequest( UUID.randomUUID(), null );
            }
        } );
    }
    
    
    @Test
    public void testEqualsReturnsCorrectlyAndHashCodeTheSameBetweenEqualInstances()
    {
        final DataPolicyAccessRequest request1 = new DataPolicyAccessRequest( 
                UUID.randomUUID(), UUID.randomUUID() );
        final DataPolicyAccessRequest request2 = new DataPolicyAccessRequest(
                request1.getUserId(), request1.getDataPolicyId() );
        final DataPolicyAccessRequest request3 = new DataPolicyAccessRequest(
                UUID.randomUUID(), request1.getDataPolicyId() );
        final DataPolicyAccessRequest request4 = new DataPolicyAccessRequest(
                request1.getUserId(), UUID.randomUUID() );

        assertTrue(request1.equals( request1 ), "Equals shoulda returned correctly.");
        assertTrue(request1.equals( request2 ), "Equals shoulda returned correctly.");
        assertFalse(request1.equals( request3 ), "Equals shoulda returned correctly.");
        assertFalse(request1.equals( request4 ), "Equals shoulda returned correctly.");
        assertFalse(request1.equals(this), "Equals shoulda returned correctly.");
        assertFalse(request1.equals( ( DataPolicyAccessRequest.class == request1.getClass() ) ? null : this), "Equals shoulda returned correctly.");

        final Object expected1 = request1.hashCode();
        assertEquals(expected1, request1.hashCode(), "Hash code shoulda returned the same between equal instances.");
        final Object expected = request1.hashCode();
        assertEquals(expected, request2.hashCode(), "Hash code shoulda returned the same between equal instances.");
        assertFalse(request1.hashCode() == request3.hashCode(), "Hash code shoulda returned the same between equal instances.");
        assertFalse(request1.hashCode() == request4.hashCode(), "Hash code shoulda returned the same between equal instances.");
        assertFalse(request1.hashCode() == this.hashCode(), "Hash code shoulda returned the same between equal instances.");
    }
}
