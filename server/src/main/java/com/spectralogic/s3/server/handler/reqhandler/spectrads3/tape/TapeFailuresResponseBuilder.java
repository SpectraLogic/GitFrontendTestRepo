/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.rpc.dataplanner.domain.TapeFailureInformation;
import com.spectralogic.s3.common.rpc.dataplanner.domain.TapeFailuresInformation;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.marshal.CustomMarshaledName;

final class TapeFailuresResponseBuilder
{
    TapeFailuresResponseBuilder( 
            final TapeFailuresInformation tapeFailures, 
            final CommandExecutionParams params )
    {
        m_tapeFailures = tapeFailures;
        m_params = params;
        Validations.verifyNotNull( "Tape failures", m_tapeFailures );
        Validations.verifyNotNull( "Params", m_params );
    }
    

    TapeFailuresApiBean build()
    {
        final Set< UUID > tapeIds = new HashSet<>();
        final Map< UUID, String > failures = new HashMap<>();
        for ( final TapeFailureInformation info : m_tapeFailures.getFailures() )
        {
            tapeIds.add( info.getTapeId() );
            failures.put( info.getTapeId(), info.getFailure() );
        }

        final List< Tape > tapes = 
                m_params.getServiceManager().getRetriever( Tape.class ).retrieveAll( tapeIds ).toList();
        Collections.sort( 
                tapes,
                new BeanComparator<>( Tape.class, Tape.BAR_CODE ) );
        
        final List< TapeFailureApiBean > retval = new ArrayList<>();
        for ( final Tape tape : tapes )
        {
            retval.add( BeanFactory.newBean( TapeFailureApiBean.class )
                    .setTape( tape )
                    .setCause( failures.get( tape.getId() ) ) );
        }
        
        return BeanFactory.newBean( TapeFailuresApiBean.class ).setFailures( 
                CollectionFactory.toArray( TapeFailureApiBean.class, retval ) );
    }
    
    
    interface TapeFailuresApiBean extends SimpleBeanSafeToProxy
    {
        String FAILURES = "failures";
        
        @CustomMarshaledName( "failure" )
        TapeFailureApiBean [] getFailures();
        
        TapeFailuresApiBean setFailures( final TapeFailureApiBean [] value );
    } // end inner class def
    
    
    interface TapeFailureApiBean extends SimpleBeanSafeToProxy
    {
        String TAPE = "tape";
        
        Tape getTape();
        
        TapeFailureApiBean setTape( final Tape value );
        
        
        String CAUSE = "cause";
        
        String getCause();
        
        TapeFailureApiBean setCause( final String value );
    } // end inner class def
    
    
    private final TapeFailuresInformation m_tapeFailures;
    private final CommandExecutionParams m_params;
}
