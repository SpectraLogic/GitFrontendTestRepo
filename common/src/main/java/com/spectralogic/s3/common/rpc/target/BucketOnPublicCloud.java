/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.target;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.rpc.tape.domain.BucketOnMedia;

public interface BucketOnPublicCloud extends BucketOnMedia
{
    String NEXT_MARKER = "nextMarker";
    
    /**
     * @return null if this is the last set of results; non-null if there are more results
     */
    String getNextMarker();
    
    BucketOnPublicCloud setNextMarker( final String value );
    
    
    String FAILURES = "failures";
    
    /**
     * @return {@code Map <object id, Set <failures for object id >> }
     */
    Map< UUID, Set< Exception > > getFailures();
    
    void setFailures( final Map< UUID, Set< Exception > > value );
}
