/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.shared;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.References;

public interface ImportTapeTargetDirective< T > extends ImportPersistenceTargetDirective< T >
{
    String TAPE_ID = "tapeId";
    
    @CascadeDelete
    @References( Tape.class )
    UUID getTapeId();
    
    T setTapeId( final UUID value );
}
