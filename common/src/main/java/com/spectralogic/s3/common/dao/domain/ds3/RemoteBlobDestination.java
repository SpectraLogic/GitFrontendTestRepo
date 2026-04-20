/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.UUID;

import com.spectralogic.util.bean.lang.*;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
import com.spectralogic.util.db.lang.References;

public interface RemoteBlobDestination< T > extends SimpleBeanSafeToProxy
{
    String ENTRY_ID = "entryId";

    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    @References( JobEntry.class )
    UUID getEntryId();
    
    T setEntryId(final UUID value );
    
    
    String TARGET_ID = "targetId";
    
    UUID getTargetId();
    
    T setTargetId( final UUID value );


    String RULE_ID = "ruleId";

    UUID getRuleId();

    T setRuleId( final UUID value );


    String BLOB_STORE_STATE = "blobStoreState";

    @DefaultEnumValue( "PENDING" )
    JobChunkBlobStoreState getBlobStoreState();

    T setBlobStoreState( final JobChunkBlobStoreState value );
}
