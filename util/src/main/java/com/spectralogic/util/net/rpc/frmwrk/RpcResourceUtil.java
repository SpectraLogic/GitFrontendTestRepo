/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.frmwrk;

import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.cache.CacheResultProvider;
import com.spectralogic.util.cache.StaticCache;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.exception.FailureType;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.marshal.MarshalUtil;

public final class RpcResourceUtil
{
    private RpcResourceUtil()
    {
        // singleton
    }
    
    
    public static void verifyRpcMethodInvocationDoesNotViolateNullParamContracts(
            final Method method,
            final Object [] args )
    {
        if ( null == args )
        {
            return;
        }
        
        final Annotation [][] paramAnnotations = method.getParameterAnnotations();
        for ( int i = 0; i < args.length; ++i )
        {
            if ( null == args[ i ] )
            {
                boolean found = false;
                for ( final Annotation a : paramAnnotations[ i ] )
                {
                    if ( NullAllowed.class == a.annotationType() )
                    {
                        found = true;
                    }
                }
                if ( !found )
                {
                    throw new IllegalArgumentException(
                            "Parameter number " + i + " of " + method + " cannot be null." );
                }
            }
        }
    }
    
    
    public static Class< ? extends RpcResource > getApi( 
            final Class< ? extends RpcResource > rpcResourceOrApi )
    {
        final Set< Class< ? > > candidates = API_CACHE.get( rpcResourceOrApi );
        
        if ( candidates.isEmpty() )
        {
            throw new IllegalArgumentException(
                    rpcResourceOrApi + " does not implement an interface that has annotation " 
                    + RpcResourceName.class.getSimpleName() + "." );
        }
        if ( 1 < candidates.size() )
        {
            throw new IllegalArgumentException( 
                    "Multiple " + RpcResourceName.class.getSimpleName() + " candidates found for "
                    + rpcResourceOrApi + ": " + candidates );
        }
        
        @SuppressWarnings( "unchecked" )
        final Class< ? extends RpcResource > retval =
                (Class< ? extends RpcResource >)candidates.iterator().next();
        if ( !retval.isInterface() )
        {
            throw new IllegalArgumentException( 
                    "The RPC resource API must be an interface: " + retval );
        }
        if ( !RpcResource.class.isAssignableFrom( retval ) )
        {
            throw new IllegalArgumentException(
                    "The RPC resource API does not implement " + RpcResource.class.getSimpleName() 
                    + ": " + retval );
        }
        
        return retval;
    }
    
    
    private final static class ApiProvider implements CacheResultProvider< Class< ? >, Set< Class< ? > > >
    {
        public Set< Class< ? > > generateCacheResultFor( final Class< ? > rpcResourceOrApi )
        {
            final Set< Class< ? > > retval = new HashSet<>();
            if ( null == rpcResourceOrApi )
            {
                return retval;
            }
            if ( null != rpcResourceOrApi.getAnnotation( RpcResourceName.class ) )
            {
                retval.add( rpcResourceOrApi );
            }
            
            for ( final Class< ? > candidate : rpcResourceOrApi.getInterfaces() )
            {
                retval.addAll( API_CACHE.get( candidate ) );
            }
            retval.addAll( API_CACHE.get( rpcResourceOrApi.getSuperclass() ) );
            
            return retval;
        }
    } // end inner class def
    
    
    public static void validate( final Class< ? extends RpcResource > rpcResourceOrApi )
    {
        final Class< ? extends RpcResource > rpcApi = getApi( rpcResourceOrApi );
        
        for ( final Method m : rpcResourceOrApi.getMethods() )
        {
            if ( RpcFuture.class != m.getReturnType() )
            {
                if ( RpcFuture.class.isAssignableFrom( m.getReturnType() ) )
                {
                    throw new RuntimeException( 
                            "RPC method " + m + " should return a " + RpcFuture.class 
                            + " and not a " + m.getReturnType() + "." );
                }
                continue;
            }
            
            final Method rpcApiMethod;
            try
            {
                rpcApiMethod = rpcApi.getMethod( m.getName(), m.getParameterTypes() );
            }
            catch ( final Exception ex )
            {
                throw new IllegalArgumentException( 
                        "RPC methods must be visible on " + rpcApi + ".  This one is not: " + m, ex );
            }
            
            if ( null == rpcApiMethod.getAnnotation( RpcMethodReturnType.class ) )
            {
                throw new IllegalArgumentException( 
                        "Every RPC resource API method must have annotation "
                        + RpcMethodReturnType.class.getSimpleName()
                        + ".  The following method is in violation: " + rpcApiMethod );
            }
            validateVariableLengthParamIsLastParam( rpcApiMethod );
        }
    }
    
    
    private static void validateVariableLengthParamIsLastParam( final Method m )
    {
        int index = 0;
        for ( final Class< ? > type : m.getParameterTypes() )
        {
            if ( ++index == m.getParameterTypes().length )
            {
                return;
            }
            if ( type.isArray() )
            {
                throw new UnsupportedOperationException( 
                        "Only the last parameter of an RPC method can be of variable length.  "
                         + "Method is in violation: " + m );
            }
        }
    }
    
    
    public static void validateResponse( final Object response, final FailureType failureIfValidationFails )
    {
        Validations.verifyNotNull( "Failure if validation fails", failureIfValidationFails );
        if ( null == response )
        {
            return;
        }
        if ( !SimpleBeanSafeToProxy.class.isAssignableFrom( response.getClass() ) )
        {
            return;
        }
        
        for ( final String prop : BeanUtils.getPropertyNames( response.getClass() ) )
        {
            final Method reader = BeanUtils.getReader( response.getClass(), prop );
            if ( null == reader )
            {
                continue;
            }
            
            try
            {
                final Object value = reader.invoke( response );
                if ( null == value
                        || ( reader.getReturnType().isArray() && 0 == Array.getLength( value ) )
                        || ( Collection.class.isAssignableFrom( reader.getReturnType() ) 
                                && ( (Collection<?>)value ).isEmpty() ) )
                {
                    if ( null != reader.getAnnotation( Optional.class ) )
                    {
                        continue;
                    }
                    throw new DaoException(
                            failureIfValidationFails,
                            "Bean property '" + prop + "' on " + response + " cannot be null or empty." );
                }
                
                if ( SimpleBeanSafeToProxy.class.isAssignableFrom( value.getClass() ) )
                {
                    validateResponse( value, failureIfValidationFails );
                }
                if ( value.getClass().isArray() )
                {
                    for ( final Object e : ReflectUtil.toObjectArray( value ) )
                    {
                        validateResponse( e, failureIfValidationFails );
                    }
                }
                if ( Collection.class.isAssignableFrom( value.getClass() ) )
                {
                    for ( final Object e : (Collection< ? >)value )
                    {
                        validateResponse( e, failureIfValidationFails );
                    }
                }
            }
            catch ( final Exception ex )
            {
                throw ExceptionUtil.toRuntimeException( ex );
            }
        }
    }
    
    
    public static String getResourceName( Class< ? extends RpcResource > rpcResourceOrApi )
    {
        rpcResourceOrApi = getApi( rpcResourceOrApi );
        return NamingConventionType.UNDERSCORED.convert( 
                rpcResourceOrApi.getAnnotation( RpcResourceName.class ).value() );
    }
    
    
    public static List< Object > getMethodInvokeParams( 
            final Method method,
            final List< String > requestParameters )
    {
        final int numberOfMethodParameters = method.getParameterTypes().length;
        final boolean isVariableParameter = ( 0 == numberOfMethodParameters ) ? 
                false 
                : method.getParameterTypes()[ numberOfMethodParameters - 1 ].isArray();
        final List< Object > methodInvokeParams = new ArrayList<>();
        final int numberOfNonVariableParameters = ( isVariableParameter ) ?
                numberOfMethodParameters - 1 
                : numberOfMethodParameters;
        for ( int i = 0; i < numberOfNonVariableParameters; ++i )
        {
            methodInvokeParams.add( MarshalUtil.getTypedValueFromNullableString( 
                    method.getParameterTypes()[ i ], 
                    requestParameters.get( i ) ) );
        }
        if ( isVariableParameter )
        {
            final List< Object > variableParameters = new ArrayList<>();
            final Class< ? > componentType = 
                    method.getParameterTypes()[ numberOfMethodParameters - 1 ].getComponentType();
            for ( int i = numberOfNonVariableParameters; i < requestParameters.size(); ++i )
            {
                final Object arrayElementValue = MarshalUtil.getTypedValueFromNullableString( 
                        componentType, 
                        requestParameters.get( i ) );
                variableParameters.add( arrayElementValue );
            }
            
            final Object array = Array.newInstance( componentType, variableParameters.size() );
            for ( int i = 0; i < variableParameters.size(); ++i )
            {
                if ( null == variableParameters.get( i ) )
                {
                    continue;
                }
                Array.set( array, i, variableParameters.get( i ) );
            }
            methodInvokeParams.add( array );
        }
        
        return methodInvokeParams;
    }
    
    
    private final static StaticCache< Class< ? >, Set< Class< ? > > > API_CACHE =
            new StaticCache<>( new ApiProvider() );
}
