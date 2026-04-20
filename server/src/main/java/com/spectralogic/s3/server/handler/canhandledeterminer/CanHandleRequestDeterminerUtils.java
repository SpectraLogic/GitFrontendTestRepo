/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.canhandledeterminer;

import com.spectralogic.util.lang.Validations;

final class CanHandleRequestDeterminerUtils
{
    private CanHandleRequestDeterminerUtils()
    {
        // singleton
    }
    
    
    static String sanitizeSampleUrl( final String url )
    {
        Validations.verifyNotNull( "Url", url );
        final int qIndex = url.indexOf( '?' );
        if ( 0 > qIndex )
        {
            return url;
        }
        final String prefix = url.substring( 0, qIndex + 1 );
        final String suffix = url.substring( qIndex + 1 ).replace( "?", "&" );
        return prefix + suffix;
    }
}
