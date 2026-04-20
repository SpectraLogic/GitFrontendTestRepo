/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.api;

import java.util.Set;
import java.util.UUID;

public interface TapeAvailability
{
    UUID getTapePartitionId();


    UUID getDriveId();

    
    UUID getTapeInDrive();
    

    Set< UUID > getAvailableTapes();

    
    /**
     * @return tapes that are unavailable since they are either (i) locked or (ii) in a different partition
     * from the one that the available tape drive is in (and for which tape availability is based on)
     */
    Set< UUID > getTemporarilyUnavailableTapes();

    
    /**
     * @return tapes that are unavailable since they are in an offline, lost, or ejected state (these tapes
     * may never come online and thus should be treated as permanently unavailable)
     */
    Set< UUID > getPermanentlyUnavailableTapes();

    
    Set< UUID > getAllUnavailableTapes();
    
    
    /**
     * @return the null if the tape specified is available, the reason the tape is unavailable if it is
     * temporarily unavailable, or throws an exception if either (i) it is permanently unavailable or (ii) it 
     * is unknown
     */
    String verifyAvailable( final UUID tapeId );

    /**
     * @return a string summary of tape availability
     */
    String getSummary();
}
