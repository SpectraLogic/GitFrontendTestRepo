/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.tape;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.s3.common.dao.service.shared.BaseFailureService;
import com.spectralogic.util.bean.BeanSQLOrdering;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.query.Query;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;

final class TapeFailureServiceImpl extends BaseFailureService< TapeFailure > implements TapeFailureService
{
    TapeFailureServiceImpl()
    {
        super( TapeFailure.class );
    }
    
    
    public void deleteAll( final UUID tapeId )
    {
        deleteAll( Require.beanPropertyEquals( TapeFailure.TAPE_ID, tapeId ) );
    }
    
    
    public void deleteAll( final UUID tapeId, final TapeFailureType failureType )
    {
        deleteAll( Require.all( 
                Require.beanPropertyEquals( TapeFailure.TAPE_ID, tapeId ),
                Require.beanPropertyEquals( Failure.TYPE, failureType ) ) );
    }

    @Override
    public TapeFailure retrieveMostRecentFailure(final WhereClause filter) {
        final BeanSQLOrdering ordering = new BeanSQLOrdering();
        ordering.add( TapeFailure.DATE, SortBy.Direction.DESCENDING );
        final Query.LimitableRetrievable query = Query.where(filter).orderBy(ordering);
        return retrieveAll(query).getFirst();
    }
}
