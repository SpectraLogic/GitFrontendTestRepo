/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.tape;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.tape.TapeFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.s3.common.dao.service.shared.FailureService;
import com.spectralogic.util.db.query.WhereClause;

public interface TapeFailureService extends FailureService< TapeFailure >
{
    void create( final TapeFailure failure );
    
    
    void deleteAll( final UUID tapeId );
    
    
    void deleteAll( final UUID tapeId, final TapeFailureType type );

    TapeFailure retrieveMostRecentFailure(final WhereClause filter);
}
