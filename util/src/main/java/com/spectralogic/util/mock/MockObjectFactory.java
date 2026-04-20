/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.mock;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.reflect.ReflectUtil;

/**
 * Given the type of object needed, this factory will return a non-null instance you can use
 * provided you don't care what the non-null instance is.
 */
public final class MockObjectFactory
{
    private MockObjectFactory()
    {
        // singleton
    }
    
    
    public static List< Object > objectsForTypes( final List< Class< ? > > classes )
    {
        Validations.verifyNotNull( "Classes", classes );
        
        final List< Object > retval = new ArrayList<>();
        for ( final Class< ? > clazz : classes )
        {
            retval.add( objectForType( clazz ) );
        }
        return retval;
    }
    
    
    public static Object objectForType( Class< ? > clazz )
    {
        Validations.verifyNotNull( "Class", clazz );
        
        if ( clazz.isPrimitive() )
        {
            clazz = ReflectUtil.toNonPrimitiveType( clazz );
        }
        
        if ( Integer.class == clazz )
        {
            return Integer.valueOf( 0 );
        }
        if ( Long.class == clazz )
        {
            return Long.valueOf( 0 );
        }
        if ( Double.class == clazz )
        {
            return Double.valueOf( 0 );
        }
        if ( String.class == clazz )
        {
            return "";
        }
        if ( Object.class == clazz )
        {
            return new Object();
        }
        if ( Boolean.class == clazz )
        {
            return Boolean.FALSE;
        }
        if ( UUID.class == clazz )
        {
            return UUID.fromString( "29714d1e-0a60-48f2-9203-8de42ccb270d" );
        }
        if ( Date.class == clazz )
        {
            return new Date( 10030 );
        }
        
        if ( clazz.isArray() )
        {
            return Array.newInstance( clazz.getComponentType(), 0 );
        }
        if ( clazz.isInterface() )
        {
            return InterfaceProxyFactory.getProxy( clazz, null );
        }
        if ( clazz.isEnum() )
        {
            return clazz.getEnumConstants()[ 0 ];
        }
        
        throw new UnsupportedOperationException( "Cannot support " + clazz );
    }
}
