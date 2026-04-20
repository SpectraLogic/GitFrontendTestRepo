/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeAvailability;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeLockSupport;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class TapeAvailabilityImpl_Test 
{
    @Test
    public void testComplexConstructorPreferredTapeIsNotAvailableResultsInNoPreferredTape()
    {
        final UUID preferredTape = UUID.randomUUID();
        final TapeDrive drive = BeanFactory.newBean( TapeDrive.class ).setTapeId( preferredTape );
        final UUID otherTape = UUID.randomUUID();
        final UUID lockedTape = UUID.randomUUID();
        final UUID lockedTapeUnavailable = UUID.randomUUID();
        final UUID otherPartitionTape = UUID.randomUUID();
        final UUID otherPartitionTapeUnavailable = UUID.randomUUID();
        final UUID unavailableTape = UUID.randomUUID();

        final TapeAvailability ta = new TapeAvailabilityImpl(
                drive,
                CollectionFactory.toSet( 
                        preferredTape, otherTape, lockedTape, lockedTapeUnavailable, unavailableTape ),
                mockTapeLockSupport( CollectionFactory.toSet( 
                        lockedTape, lockedTapeUnavailable ), CollectionFactory.toSet() ),
                CollectionFactory.toSet( 
                        otherPartitionTape, otherPartitionTapeUnavailable ),
                CollectionFactory.toSet( 
                        lockedTapeUnavailable, otherPartitionTapeUnavailable,
                        unavailableTape, preferredTape ),
                new HashSet<>(), new HashSet<>(), true);
        assertNull( ta.getTapeInDrive(), "Should notta reported preferred tape since it's not available.");
    }
    
    
   @Test
    public void testComplexConstructorReportsCorrectTapeAvailability()
    {
        final UUID preferredTape = UUID.randomUUID();
        final TapeDrive drive = BeanFactory.newBean( TapeDrive.class ).setTapeId( preferredTape );
        final UUID otherTape = UUID.randomUUID();
        final UUID lockedTape = UUID.randomUUID();
        final UUID lockedTapeUnavailable = UUID.randomUUID();
        final UUID otherPartitionTape = UUID.randomUUID();
        final UUID otherPartitionTapeUnavailable = UUID.randomUUID();
        final UUID unavailableTape = UUID.randomUUID();
        
        final TapeAvailability ta = new TapeAvailabilityImpl(
                drive,
                CollectionFactory.toSet( 
                        preferredTape, otherTape, lockedTape, lockedTapeUnavailable, unavailableTape ),
                mockTapeLockSupport( CollectionFactory.toSet( 
                        lockedTape, lockedTapeUnavailable ), CollectionFactory.toSet() ),
                CollectionFactory.toSet( 
                        otherPartitionTape, otherPartitionTapeUnavailable ),
                CollectionFactory.toSet( 
                        lockedTapeUnavailable, otherPartitionTapeUnavailable, unavailableTape ),
                new HashSet<>(), new HashSet<>(), true);
        final Object expected3 = CollectionFactory.toSet( preferredTape, otherTape );
        assertEquals(expected3, ta.getAvailableTapes(), "Shoulda reported tape availability correctly.");
        assertEquals(preferredTape, ta.getTapeInDrive(), "Shoulda reported tape availability correctly.");
        final Object expected2 = CollectionFactory.toSet(
                lockedTapeUnavailable, otherPartitionTapeUnavailable, unavailableTape );
        assertEquals(expected2, ta.getPermanentlyUnavailableTapes(), "Shoulda reported tape availability correctly.");
        final Object expected1 = CollectionFactory.toSet( lockedTape, otherPartitionTape );
        assertEquals(expected1, ta.getTemporarilyUnavailableTapes(), "Shoulda reported tape availability correctly.");
        final Object expected = CollectionFactory.toSet(
                lockedTape, lockedTapeUnavailable,
                otherPartitionTape, otherPartitionTapeUnavailable, unavailableTape );
        assertEquals(expected, ta.getAllUnavailableTapes(), "Shoulda reported tape availability correctly.");
    }
    
    
   @Test
    public void testSimpleConstructorNullDriveIdNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new TapeAvailabilityImpl( UUID.randomUUID(), null, UUID.randomUUID() );
            }
        } );
    }
    
    
   @Test
    public void testSimpleConstructorNullTapeIdNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new TapeAvailabilityImpl( UUID.randomUUID(), UUID.randomUUID(), null );
            }
        } );
    }
    
    
   @Test
    public void testSimpleConstructorNullPartitionIdNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new TapeAvailabilityImpl( null, UUID.randomUUID(), UUID.randomUUID() );
            }
        } );
    }
    
    
   @Test
    public void testSimpleConstructorReportsCorrectTapeAvailability()
    {
        final UUID tapePartitionId = UUID.randomUUID();
        final UUID preferredTape = UUID.randomUUID();
        final TapeDrive drive = BeanFactory.newBean( TapeDrive.class ).setTapeId( preferredTape );
        drive.setId( UUID.randomUUID() );
        drive.setPartitionId( tapePartitionId );
        final TapeAvailability ta = new TapeAvailabilityImpl( 
                drive.getPartitionId(), drive.getId(), drive.getTapeId() );
        final Object expected3 = CollectionFactory.toSet( preferredTape );
        assertEquals(expected3, ta.getAvailableTapes(), "Shoulda reported tape availability correctly.");
        assertEquals(preferredTape, ta.getTapeInDrive(), "Shoulda reported tape availability correctly.");
        assertEquals(tapePartitionId, ta.getTapePartitionId(), "Shoulda reported tape availability correctly.");
        final Object expected2 = CollectionFactory.toSet();
        assertEquals(expected2, ta.getPermanentlyUnavailableTapes(), "Shoulda reported tape availability correctly.");
        final Object expected1 = CollectionFactory.toSet();
        assertEquals(expected1, ta.getTemporarilyUnavailableTapes(), "Shoulda reported tape availability correctly.");
        final Object expected = CollectionFactory.toSet();
        assertEquals(expected, ta.getAllUnavailableTapes(), "Shoulda reported tape availability correctly.");
    }
    
    
   @Test
    public void testVerifyAvailableReturnsNullIfItIsAvailable()
    {
        final UUID preferredTape = UUID.randomUUID();
        final TapeDrive drive = BeanFactory.newBean( TapeDrive.class ).setTapeId( preferredTape );
        final UUID otherTape = UUID.randomUUID();
        final UUID lockedTape = UUID.randomUUID();
        final UUID lockedTapeUnavailable = UUID.randomUUID();
        final UUID otherPartitionTape = UUID.randomUUID();
        final UUID otherPartitionTapeUnavailable = UUID.randomUUID();
        final UUID unavailableTape = UUID.randomUUID();
        
        final TapeAvailability ta = new TapeAvailabilityImpl(
                drive,
                CollectionFactory.toSet( 
                        preferredTape, otherTape, lockedTape, lockedTapeUnavailable, unavailableTape ),
                mockTapeLockSupport( CollectionFactory.toSet( 
                        lockedTape, lockedTapeUnavailable ), CollectionFactory.toSet() ),
                CollectionFactory.toSet( 
                        otherPartitionTape, otherPartitionTapeUnavailable ),
                CollectionFactory.toSet( 
                        lockedTapeUnavailable, otherPartitionTapeUnavailable, unavailableTape ),
                new HashSet<>(), new HashSet<>(), true);
        assertNull((Object) ta.verifyAvailable(otherTape), "Shoulda returned null since tape available.");
    }
    
    
   @Test
    public void testVerifyAvailableReturnsNonNullIfItIsTemporarilyUnavailable()
    {
        final UUID preferredTape = UUID.randomUUID();
        final TapeDrive drive = BeanFactory.newBean( TapeDrive.class ).setTapeId( preferredTape );
        final UUID otherTape = UUID.randomUUID();
        final UUID lockedTape = UUID.randomUUID();
        final UUID lockedTapeUnavailable = UUID.randomUUID();
        final UUID otherPartitionTape = UUID.randomUUID();
        final UUID otherPartitionTapeUnavailable = UUID.randomUUID();
        final UUID unavailableTape = UUID.randomUUID();
        
        final TapeAvailability ta = new TapeAvailabilityImpl(
                drive,
                CollectionFactory.toSet( 
                        preferredTape, otherTape, lockedTape, lockedTapeUnavailable, unavailableTape ),
                mockTapeLockSupport( CollectionFactory.toSet( 
                        lockedTape, lockedTapeUnavailable ), CollectionFactory.toSet() ),
                CollectionFactory.toSet( 
                        otherPartitionTape, otherPartitionTapeUnavailable ),
                CollectionFactory.toSet( 
                        lockedTapeUnavailable, otherPartitionTapeUnavailable, unavailableTape ),
                new HashSet<>(), new HashSet<>(), true);
        assertNotNull(
                "Shoulda returned non-null since tape not available.",
                ta.verifyAvailable( lockedTape ) );
        assertNotNull(
                "Shoulda returned non-null since tape not available.",
                ta.verifyAvailable( otherPartitionTape ) );
    }

   @Test
    public void testVerifyAvailableReturnsNonNullForRecentlyUnlockedUnlessAllowed()
    {
        final UUID preferredTape = UUID.randomUUID();
        final TapeDrive drive = BeanFactory.newBean( TapeDrive.class ).setTapeId( preferredTape );
        final UUID otherTape = UUID.randomUUID();
        final UUID lockedTape = UUID.randomUUID();
        final UUID recentlyUnlockedTape = UUID.randomUUID();
        final UUID lockedTapeUnavailable = UUID.randomUUID();
        final UUID otherPartitionTape = UUID.randomUUID();
        final UUID otherPartitionTapeUnavailable = UUID.randomUUID();
        final UUID unavailableTape = UUID.randomUUID();

        TapeAvailability ta = new TapeAvailabilityImpl(
                drive,
                CollectionFactory.toSet(
                        preferredTape, otherTape, lockedTape, lockedTapeUnavailable, unavailableTape, recentlyUnlockedTape ),
                mockTapeLockSupport( CollectionFactory.toSet(
                        lockedTape, lockedTapeUnavailable ), CollectionFactory.toSet(recentlyUnlockedTape) ),
                CollectionFactory.toSet(
                        otherPartitionTape, otherPartitionTapeUnavailable ),
                CollectionFactory.toSet(
                        lockedTapeUnavailable, otherPartitionTapeUnavailable, unavailableTape ),
                new HashSet<>(), new HashSet<>(), true);
        assertNull((Object) ta.verifyAvailable(recentlyUnlockedTape), "Shoulda returned null since recently unlocked tapes allowed.");
        ta = new TapeAvailabilityImpl(
                drive,
                CollectionFactory.toSet(
                        preferredTape, otherTape, lockedTape, lockedTapeUnavailable, unavailableTape, recentlyUnlockedTape ),
                mockTapeLockSupport( CollectionFactory.toSet(
                        lockedTape, lockedTapeUnavailable ), CollectionFactory.toSet(recentlyUnlockedTape) ),
                CollectionFactory.toSet(
                        otherPartitionTape, otherPartitionTapeUnavailable ),
                CollectionFactory.toSet(
                        lockedTapeUnavailable, otherPartitionTapeUnavailable, unavailableTape ),
                new HashSet<>(), new HashSet<>(), false);
        assertNotNull(
                "Shoulda returned non-null since recently unlocked tapes not allowed.",
                ta.verifyAvailable( recentlyUnlockedTape ) );
    }
    
   @Test
    public void testTapeInOfflineDriveConsideredTemporarilyUnavailable()
    {
        final UUID preferredTape = UUID.randomUUID();
        final TapeDrive drive = BeanFactory.newBean( TapeDrive.class ).setTapeId( preferredTape );
        final UUID otherTape = UUID.randomUUID();
        final UUID lockedTape = UUID.randomUUID();
        final UUID lockedTapeUnavailable = UUID.randomUUID();
        final UUID otherPartitionTape = UUID.randomUUID();
        final UUID otherPartitionTapeUnavailable = UUID.randomUUID();
        final UUID unavailableTape = UUID.randomUUID();
        final UUID tapeInUnknownDrive = UUID.randomUUID();
        
        final TapeAvailability ta = new TapeAvailabilityImpl(
                drive,
                CollectionFactory.toSet( 
                        preferredTape, otherTape, lockedTape, lockedTapeUnavailable, unavailableTape ),
                mockTapeLockSupport( CollectionFactory.toSet( 
                        lockedTape, lockedTapeUnavailable ), CollectionFactory.toSet() ),
                CollectionFactory.toSet( 
                        otherPartitionTape, otherPartitionTapeUnavailable ),
                CollectionFactory.toSet( 
                        lockedTapeUnavailable, otherPartitionTapeUnavailable, unavailableTape ),
                CollectionFactory.toSet(
                		tapeInUnknownDrive ), new HashSet<>(), true);
        assertNotNull(
                "Shoulda returned non-null since tape not available.",
                ta.verifyAvailable( tapeInUnknownDrive ) );
    }
    
    
   @Test
    public void testVerifyAvailableThrowsIfItIsPermanentlyUnavailable()
    {
        final UUID preferredTape = UUID.randomUUID();
        final TapeDrive drive = BeanFactory.newBean( TapeDrive.class ).setTapeId( preferredTape );
        final UUID otherTape = UUID.randomUUID();
        final UUID lockedTape = UUID.randomUUID();
        final UUID lockedTapeUnavailable = UUID.randomUUID();
        final UUID otherPartitionTape = UUID.randomUUID();
        final UUID otherPartitionTapeUnavailable = UUID.randomUUID();
        final UUID unavailableTape = UUID.randomUUID();
        
        final TapeAvailability ta = new TapeAvailabilityImpl(
                drive,
                CollectionFactory.toSet( 
                        preferredTape, otherTape, lockedTape, lockedTapeUnavailable, unavailableTape ),
                mockTapeLockSupport( CollectionFactory.toSet( 
                        lockedTape, lockedTapeUnavailable ), CollectionFactory.toSet() ),
                CollectionFactory.toSet( 
                        otherPartitionTape, otherPartitionTapeUnavailable ),
                CollectionFactory.toSet( 
                        lockedTapeUnavailable, otherPartitionTapeUnavailable, unavailableTape ),
                new HashSet<>(), new HashSet<>(), true);

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                ta.verifyAvailable( unavailableTape );
            }
        } );
    }
    
    
   @Test
    public void testVerifyAvailableThrowsIfItIsUnknown()
    {
        final UUID preferredTape = UUID.randomUUID();
        final TapeDrive drive = BeanFactory.newBean( TapeDrive.class ).setTapeId( preferredTape );
        final UUID otherTape = UUID.randomUUID();
        final UUID lockedTape = UUID.randomUUID();
        final UUID lockedTapeUnavailable = UUID.randomUUID();
        final UUID otherPartitionTape = UUID.randomUUID();
        final UUID otherPartitionTapeUnavailable = UUID.randomUUID();
        final UUID unavailableTape = UUID.randomUUID();
        
        final TapeAvailability ta = new TapeAvailabilityImpl(
                drive,
                CollectionFactory.toSet( 
                        preferredTape, otherTape, lockedTape, lockedTapeUnavailable, unavailableTape ),
                mockTapeLockSupport( CollectionFactory.toSet( 
                        lockedTape, lockedTapeUnavailable ), CollectionFactory.toSet()),
                CollectionFactory.toSet( 
                        otherPartitionTape, otherPartitionTapeUnavailable ),
                CollectionFactory.toSet( 
                        lockedTapeUnavailable, otherPartitionTapeUnavailable, unavailableTape ),
                new HashSet<>(), new HashSet<>(), true);

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                ta.verifyAvailable( UUID.randomUUID() );
            }
        } );
    }


    private TapeLockSupport< ? > mockTapeLockSupport( final Set< UUID > lockedTapes, final Set< UUID > recentlyUnlocked)
    {
        final TapeLockSupport retval = mock(TapeLockSupport.class);
        when(retval.getLockedTapes(any())).thenReturn(lockedTapes);
        when(retval.getRecentlyUnlocked()).thenReturn(recentlyUnlocked);
        when(retval.getTapeLockHolder(any(UUID.class))).thenAnswer(
                invocation -> lockedTapes.contains( invocation.getArguments()[0] ) ? "some lockholder" : null
        );
        return retval;
    }
}
