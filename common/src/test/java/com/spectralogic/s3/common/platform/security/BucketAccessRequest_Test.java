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

import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class BucketAccessRequest_Test 
{

    @Test
    public void testConstructorNullUserIdNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
        public void test()
            {
                new BucketAccessRequest( null, UUID.randomUUID(), BucketAclPermission.values()[ 0 ] );
            }
        } );
    }
    

    @Test
    public void testConstructorNullBucketIdNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {

        public void test()
            {
                new BucketAccessRequest( UUID.randomUUID(), null, BucketAclPermission.values()[ 0 ] );
            }
        } );
    }
    

    @Test
    public void testConstructorNullAclPermissionNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BucketAccessRequest( UUID.randomUUID(), UUID.randomUUID(), null );
            }
        } );
    }
    
    
    @Test
    public void testEqualsReturnsCorrectlyAndHashCodeTheSameBetweenEqualInstances()
    {
        final BucketAccessRequest request1 = new BucketAccessRequest( 
                UUID.randomUUID(), UUID.randomUUID(), BucketAclPermission.values()[ 0 ] );
        final BucketAccessRequest request2 = new BucketAccessRequest(
                request1.getUserId(), request1.getBucketId(), request1.getPermissionRequired() );
        final BucketAccessRequest request3 = new BucketAccessRequest(
                UUID.randomUUID(), request1.getBucketId(), request1.getPermissionRequired() );
        final BucketAccessRequest request4 = new BucketAccessRequest(
                request1.getUserId(), UUID.randomUUID(), request1.getPermissionRequired() );
        final BucketAccessRequest request5 = new BucketAccessRequest(
                request1.getUserId(), request1.getBucketId(), BucketAclPermission.values()[ 1 ] );

        assertTrue(request1.equals( request1 ), "Equals shoulda returned correctly.");
        assertTrue(request1.equals( request2 ), "Equals shoulda returned correctly.");
        assertFalse(request1.equals( request3 ), "Equals shoulda returned correctly.");
        assertFalse(request1.equals( request4 ), "Equals shoulda returned correctly.");
        assertFalse(request1.equals( request5 ), "Equals shoulda returned correctly.");
        assertFalse(request1.equals(this), "Equals shoulda returned correctly.");
        assertFalse(request1.equals( ( BucketAccessRequest.class == request1.getClass() ) ? null : this), "Equals shoulda returned correctly.");

        final Object expected1 = request1.hashCode();
        assertEquals(expected1, request1.hashCode(), "Hash code shoulda returned the same between equal instances.");
        final Object expected = request1.hashCode();
        assertEquals(expected, request2.hashCode(), "Hash code shoulda returned the same between equal instances.");
        assertFalse(request1.hashCode() == request3.hashCode(), "Hash code shoulda returned the same between equal instances.");
        assertFalse(request1.hashCode() == request4.hashCode(), "Hash code shoulda returned the same between equal instances.");
        assertFalse(request1.hashCode() == request5.hashCode(), "Hash code shoulda returned the same between equal instances.");
        assertFalse(request1.hashCode() == this.hashCode(), "Hash code shoulda returned the same between equal instances.");
    }
}
