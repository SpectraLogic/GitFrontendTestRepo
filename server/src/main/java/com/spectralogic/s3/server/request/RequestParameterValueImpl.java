/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.request;

import java.util.UUID;

import com.spectralogic.s3.server.frmwrk.UserInputValidations;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterValue;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.reflect.ReflectUtil;

public final class RequestParameterValueImpl implements RequestParameterValue
{
    public RequestParameterValueImpl( final DS3Request request, final Class< ? > type, final String value )
    {
        Validations.verifyNotNull( "Request", request );
        Validations.verifyNotNull( "Type", type );
        m_request = request;
        m_type = type;
        
        if ( void.class == type )
        {
            m_value = null;
        }
        else if ( UUID.class == type )
        {
            m_value = value;
        }
        else if ( long.class == type )
        {
            m_value = Long.valueOf( value );
        }
        else if ( int.class == type )
        {
            m_value = Integer.valueOf( value );
        }
        else if ( type.isEnum() )
        {
            m_value = ReflectUtil.enumValueOf( type, value );
        }
        else if ( String.class == type )
        {
            m_value = value;
        }
        else
        {
            throw new UnsupportedOperationException( "No code for " + type + "." );
        }
        
        if ( void.class != type )
        {
            Validations.verifyNotNull( "Value cannot be null for request parameter.", m_value );
        }
    }
    
    
    public < B extends Identifiable > B getBean( final BeansRetriever< B > retrieverToDiscoverWith )
    {
        Validations.verifyNotNull( "Retriever", retrieverToDiscoverWith );
        validate( UUID.class );
        return retrieverToDiscoverWith.discover( m_value );
    }
    
    
    public UUID getUuid()
    {
        validate( UUID.class );
        return UserInputValidations.toUuid( (String)m_value );
    }


    public int getInt()
    {
        return getTypedValue( int.class ).intValue();
    }


    public long getLong()
    {
        return getTypedValue( long.class ).longValue();
    }
    
    
    public String getString()
    {
        return getTypedValue( String.class );
    }
    
    
    public < T > T getEnum( final Class< T > enumType )
    {
        final T retval = getTypedValue( enumType );
        UserInputValidations.validateUserInput( m_request, retval );
        return retval;
    }

    
    private < V > V getTypedValue( final Class< V > type )
    {
        Validations.verifyNotNull( "Type", type );
        validate( type );
        @SuppressWarnings( "unchecked" )
        final V retval = (V)m_value;
        return retval;
    }
    
    
    private void validate( final Class< ? > type )
    {
        if ( type != m_type )
        {
            throw new IllegalStateException(
                    "Cannot retrieve a " + type + " since the type of this request parameter is " 
                    + m_type + "." );
        }
    }
    
    
    private final DS3Request m_request;
    private final Object m_value;
    private final Class< ? > m_type;
}
