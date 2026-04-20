/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.capacity;

import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.shared.PoolObservable;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;

final class CapacitySummaryFilter
{
    CapacitySummaryFilter()
    {
        // singleton
    }
    
    
    static WhereClause forTape( final CapacitySummaryOptionalParams params )
    {
        return Require.all( 
                ( null == params.getTapeState() ) ?
                        null : Require.beanPropertyEquals( Tape.STATE, params.getTapeState() ),
                ( null == params.getTapeType() ) ?
                        null : Require.beanPropertyEquals( Tape.TYPE, params.getTapeType() ) );
    }
    
    
    static WhereClause forPool( final CapacitySummaryOptionalParams params )
    {
        return Require.all( 
                ( null == params.getPoolState() ) ?
                        null : Require.beanPropertyEquals( Pool.STATE, params.getPoolState() ),
                ( null == params.getPoolType() ) ?
                        null : Require.beanPropertyEquals( PoolObservable.TYPE, params.getPoolType() ),
                ( null == params.getPoolHealth() ) ?
                        null : Require.beanPropertyEquals( PoolObservable.HEALTH, params.getPoolHealth() ) );
    }
}
