/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.degradation;

import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.spectralogic.s3.server.domain.UuidSetSaxHandler;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.log.LogUtil;

final class SuspectBlobUtil
{
    private SuspectBlobUtil()
    {
        // singleton
    }
    
    
    static Set< UUID > extractIds( final DS3Request request )
    {
        final Set< UUID > retval = UuidSetSaxHandler.extractIds( request );
        if ( null == retval )
        {
            LOG.info( "IDs not specified in the request payload.  " 
                      + "Will proceed to act against all suspect blob records." );
        }
        else
        {
            LOG.info( "IDs specified in the request payload: " + LogUtil.getShortVersion( retval, 10 ) );
        }
        
        if ( null == retval && !request.hasRequestParameter( RequestParameterType.FORCE ) )
        {
            throw new S3RestException( 
                    GenericFailure.FORCE_FLAG_REQUIRED, 
                    "For safety reasons, without the force flag, it is illegal to perform any action against "
                    + "all suspect blob records at once.  You must specify "
                    + "the ids of the records you want to act against or use the force flag." );
        }
        
        return retval;
    }
    
    
    private final static Logger LOG = Logger.getLogger( SuspectBlobUtil.class );
}
