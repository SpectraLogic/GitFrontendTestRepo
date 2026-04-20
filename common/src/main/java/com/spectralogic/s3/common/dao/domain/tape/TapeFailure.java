/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.tape;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;
import com.spectralogic.util.db.lang.References;

@Indexes( @Index( TapeFailure.DATE ) )
public interface TapeFailure extends DatabasePersistable, Failure< TapeFailure, TapeFailureType >
{
    String TAPE_ID = "tapeId";
    
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    @References( Tape.class )
    UUID getTapeId();
    
    TapeFailure setTapeId( final UUID value );
    
    
    String TAPE_DRIVE_ID = "tapeDriveId";

    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    @References( TapeDrive.class )
    UUID getTapeDriveId();
    
    TapeFailure setTapeDriveId( final UUID value );
    
    
    TapeFailureType getType();
    
    TapeFailure setType( final TapeFailureType value );
}
