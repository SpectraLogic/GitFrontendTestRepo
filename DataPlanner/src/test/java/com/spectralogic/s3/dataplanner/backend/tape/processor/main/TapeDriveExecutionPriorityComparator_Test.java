/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor.main;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;



import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public final class TapeDriveExecutionPriorityComparator_Test
{
    @Test
    public void testTapeDrivesAreSortedSuchThatDrivesWithoutTapesInThemComeFirst()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        mockDaoDriver.createTapeDrive( null, "a" );
        mockDaoDriver.createTapeDrive( null, "b", tape1.getId() );
        mockDaoDriver.createTapeDrive( null, "c" );
        mockDaoDriver.createTapeDrive( null, "d", tape2.getId() );
        
        final List< TapeDrive > originalList = 
                dbSupport.getServiceManager().getRetriever( TapeDrive.class ).retrieveAll().toList();
        assertNull(
                getSortedList( originalList ).get( 0 ).getTapeId(),
                "Shoulda put drives without tapes in them first."
                 );
        assertNull(
                getSortedList( originalList ).get( 1 ).getTapeId(),
                "Shoulda put drives without tapes in them first."
                 );
        assertNotNull(
                getSortedList( originalList ).get( 2 ).getTapeId(),
                "Shoulda put drives without tapes in them first."
                 );
        assertNotNull(
                getSortedList( originalList ).get( 3 ).getTapeId(),
                "Shoulda put drives without tapes in them first."
                 );
        
        int i = 0;
        while ( ++i < 10 )
        {
            if ( !getSortedList( originalList ).equals( getSortedList( originalList ) ) )
            {
                return;
            }
        }
        Assertions.fail( "Other than whether or not the drive already contains a tape, shouldn't further sort drives." );
    }
    
    
    private List< TapeDrive > getSortedList( final List< TapeDrive > originalList )
    {
        final List< TapeDrive > retval = new ArrayList<>( originalList );
        Collections.shuffle( retval );
        Collections.sort( retval, new TapeDriveExecutionPriorityComparator() );
        return retval;
    }

    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    @BeforeAll
    public static void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    }

    @AfterEach
    public void resetDB() {
        dbSupport.reset();
    }
}
