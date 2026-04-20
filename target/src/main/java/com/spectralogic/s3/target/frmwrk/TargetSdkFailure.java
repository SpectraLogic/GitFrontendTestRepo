/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.frmwrk;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import com.spectralogic.util.exception.FailureType;

public class TargetSdkFailure implements FailureType
{
    protected TargetSdkFailure( String stringCode, final int intCode )
    {
        m_stringCode = ( null == stringCode ) ? "SDK_FAILURE_" + intCode : stringCode;
        m_intCode = intCode;
    }
    
    
    public static < T extends TargetSdkFailure > T valueOf( 
            final Class< T > clazz, 
            final String stringCode, 
            final int intCode )
    {
        final Integer iCode = Integer.valueOf( intCode );
        synchronized ( INSTANCES )
        {
            if ( !INSTANCES.containsKey( clazz ) )
            {
                INSTANCES.put( clazz, new HashMap< Integer, Map< String, TargetSdkFailure > >() );
            }
            if ( !INSTANCES.get( clazz ).containsKey( iCode ) )
            {
                INSTANCES.get( clazz ).put( iCode, new HashMap< String, TargetSdkFailure >() );
            }
            
            @SuppressWarnings( "unchecked" )
            final T retval = (T)INSTANCES.get( clazz ).get( iCode ).get( stringCode );
            if ( null == retval )
            {
                try
                {
                    final Constructor< T > con = clazz.getDeclaredConstructor( String.class, int.class );
                    con.setAccessible( true );
                    INSTANCES.get( clazz ).get( iCode ).put( 
                            stringCode,
                            con.newInstance( stringCode, Integer.valueOf( intCode ) ) );
                }
                catch ( final Exception ex )
                {
                    throw new RuntimeException( ex );
                }
                return valueOf( clazz, stringCode, intCode );
            }
            
            return retval;
        }
    }
    
    
    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + m_intCode + "]";
    }


    public int getHttpResponseCode()
    {
        return m_intCode;
    }


    public String getCode()
    {
        return m_stringCode;
    }
    
    
    private final String m_stringCode;
    private final int m_intCode;
    private final static Map< Class< ? >, Map< Integer, Map< String, TargetSdkFailure > > > INSTANCES = 
            new HashMap<>();
}
