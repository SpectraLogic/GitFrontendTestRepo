/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang;

import java.util.Collection;

public final class Validations
{
    private Validations()
    {
        // singleton
    }
    
    
    public static void verifyNotNull( final String paramName, final Object param )
    {
        if ( null == paramName )
        {
            throw new IllegalArgumentException( "Param name cannot be null." );
        }
        if ( null == param )
        {
            throw new IllegalArgumentException( paramName + " cannot be null." );
        }
    }


    public static void verifyNotEmptyString( final String paramName, final String param )
    {
        verifyNotNull( paramName, "verify paramName" );
        if ( 0 == param.length() )
        {
            throw new IllegalArgumentException( paramName + " cannot be empty." );
        }
    }
    
    
    public static void verifyNotEmptyCollection( final String paramName, final Collection< ? > collection )
    {
        verifyNotNull( paramName, collection );
        if ( collection.isEmpty() )
        {
            throw new IllegalArgumentException( paramName + " cannot be empty." );
        }
    }
    
    
    public static void verifyInRange( 
            final String paramName, 
            final int min, 
            final int max, 
            final int param )
    {
        verifyInRange( paramName, min, max, Integer.valueOf( param ) );
    }
    
    
    public static void verifyInRange( 
            final String paramName, 
            final int min, 
            final int max, 
            final Integer param )
    {
        verifyNotNull( paramName, param );
        if ( param.intValue() > max )
        {
            throw new IllegalArgumentException( 
                    paramName + " cannot be greater than " + max + ", but was " + param );
        }
        if ( param.intValue() < min )
        {
            throw new IllegalArgumentException(
                    paramName + " cannot be less than " + min + ", but was " + param );
        }
    }
    
    
    public static void verifyInRange( 
            final String paramName, 
            final long min, 
            final long max, 
            final long param )
    {
        verifyInRange( paramName, min, max, Long.valueOf( param ) );
    }
    
    
    public static void verifyInRange( 
            final String paramName, 
            final long min, 
            final long max, 
            final Long param )
    {
        verifyNotNull( paramName, param );
        if ( param.longValue() > max )
        {
            throw new IllegalArgumentException( 
                    paramName + " cannot be greater than " + max + ", but was " + param );
        }
        if ( param.longValue() < min )
        {
            throw new IllegalArgumentException(
                    paramName + " cannot be less than " + min + ", but was " + param );
        }
    }
}
