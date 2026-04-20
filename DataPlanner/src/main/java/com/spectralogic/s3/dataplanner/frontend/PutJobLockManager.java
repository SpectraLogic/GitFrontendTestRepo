/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * We have validation rules surrounding object naming, so we cannot let multiple put jobs get created at
 * the same time for the same bucket, since the job creation is transact-safe and thus the objects created
 * at the beginning of the transaction won't be visible until the end of the transaction when it is committed.
 */
final class PutJobLockManager
{
    private PutJobLockManager()
    {
        //singleton
    }
    
    
    static Object getLock( final UUID bucketId )
    {
        synchronized ( LOCKS_LOCK )
        {
            if ( LOCKS.containsKey( bucketId ) )
            {
                return LOCKS.get( bucketId );
            }
            LOCKS.put( bucketId, new Object() );
            return LOCKS.get( bucketId );
        }
    }
    

    private final static Map< UUID, Object > LOCKS = new HashMap<>();
    private final static Object LOCKS_LOCK = new Object();
}
