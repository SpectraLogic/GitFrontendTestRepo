/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.shared;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
import com.spectralogic.util.db.lang.References;

public interface ReadFromObservable< T > extends SimpleBeanSafeToProxy
{
    String READ_FROM_TAPE_ID = "readFromTapeId";
    
    @Optional
    @CascadeDelete( WhenReferenceIsDeleted.SET_NULL )
    @References( Tape.class )
    UUID getReadFromTapeId();
    
    T setReadFromTapeId( final UUID value );
    
    
    String READ_FROM_POOL_ID = "readFromPoolId";
    
    @Optional
    @CascadeDelete( WhenReferenceIsDeleted.SET_NULL )
    @References( Pool.class )
    UUID getReadFromPoolId();
    
    T setReadFromPoolId( final UUID value );
    
    
    String READ_FROM_S3_TARGET_ID = "readFromS3TargetId";

    @Optional
    @CascadeDelete( WhenReferenceIsDeleted.SET_NULL )
    @References( Ds3Target.class )
    UUID getReadFromDs3TargetId();
    
    T setReadFromDs3TargetId( final UUID value );
    
    
    String READ_FROM_AZURE_TARGET_ID = "readFromAzureTargetId";

    @Optional
    @CascadeDelete( WhenReferenceIsDeleted.SET_NULL )
    @References( AzureTarget.class )
    UUID getReadFromAzureTargetId();
    
    T setReadFromAzureTargetId( final UUID value );
    
    
    String READ_FROM_DS3_TARGET_ID = "readFromDs3TargetId";

    @Optional
    @CascadeDelete( WhenReferenceIsDeleted.SET_NULL )
    @References( S3Target.class )
    UUID getReadFromS3TargetId();
    
    T setReadFromS3TargetId( final UUID value );
}
