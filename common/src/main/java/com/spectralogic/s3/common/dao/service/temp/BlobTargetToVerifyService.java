/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.temp;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.target.BlobTarget;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface BlobTargetToVerifyService< T extends BlobTarget< ? > & DatabasePersistable > 
    extends BeansRetriever< T >
{
    void verifyBegun( final UUID targetId );
    
    
    /**
     * @return Set <blob id> that are unknown to this appliance and can be deleted on the target
     */
    Set< UUID > blobsVerified( final UUID targetId, final Set< UUID > blobIds );
    
    
    void verifyCompleted( final UUID targetId );
}
