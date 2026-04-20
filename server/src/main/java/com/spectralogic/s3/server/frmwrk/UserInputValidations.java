/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.frmwrk;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.EnumWithNonUserSpecifiableConstants;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.http.RequestType;

public final class UserInputValidations
{
    public static void validateUserInput( final DS3Request request, final Object value )
    {
        if ( null == value )
        {
            return;
        }
        if ( !EnumWithNonUserSpecifiableConstants.class.isAssignableFrom( value.getClass() ) )
        {
            return;
        }
        if ( ( (EnumWithNonUserSpecifiableConstants)value ).isSpecifiableByUser() )
        {
            return;
        }
        if ( RequestType.GET == request.getHttpRequest().getType()
                || RequestType.HEAD == request.getHttpRequest().getType() )
        {
            return;
        }
        
        throw new S3RestException(
                GenericFailure.FORBIDDEN,
                "It is illegal to specify value '" + value + "' for type " 
                + value.getClass().getSimpleName() + "." );
    }
    
    
    public static UUID toUuid( final String value )
    {
        try
        {
            return UUID.fromString( value );
        }
        catch ( final RuntimeException ex )
        {
            throw new S3RestException( GenericFailure.BAD_REQUEST, "Invalid UUID: " + value, ex );
        }
    }
    
    
    public static Date toDate( final String value )
    {
        try
        {
            return new Date( value );
        }
        catch ( final RuntimeException ex )
        {
            throw new S3RestException( GenericFailure.BAD_REQUEST, "Invalid Date: " + value, ex );
        }
    }
}
