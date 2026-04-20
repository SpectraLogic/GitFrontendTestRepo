/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public final class CollectionFactory
{
    private CollectionFactory()
    {
        // singleton
    }
    
    
    public static < K, V > Map< K, V > toMap( final K key, final V value )
    {
        final HashMap< K, V > retval = new HashMap<>();
        retval.put( key, value );
        return retval;
    }
    
    
    @SafeVarargs
    public static < B > Set< B > toSet( final B ... array )
    {
        if ( null == array )
        {
            return new HashSet<>();
        }
        
        final Set< B > retval = new HashSet<>();
        for ( final B element : array )
        {
            retval.add( element );
        }
        return retval;
    }
    
    
    public static < B > B[] toArray( final Class< B > clazz, final Collection< ? extends B > collection )
    {
        Validations.verifyNotNull( "Class", clazz );
        Validations.verifyNotNull( "Collection", collection );
        
        @SuppressWarnings( "unchecked" )
        final B [] retval = (B[]) Array.newInstance( clazz, collection.size() );
        collection.toArray( retval );
        return retval;
    }
    
    
    /**
     * Arrays.asList will return a List that is backed by the array (no defensive copy).  
     * Furthermore, the list returned will not support all List interface operations.  This method
     * will return a new List not backed by the array that will support all List interface 
     * operations.
     */
    @SafeVarargs
    public static < B > List< B > toList( final B ... array )
    {
        final List< B > retval = new ArrayList<>();
        if ( null != array )
        {
            for ( final B element : array )
            {
                retval.add( element );
            }
        }
        
        return retval;
    }
    
    
    public static < T > T getDefensiveCopy( final T original )
    {
        final Object retval = getDefensiveCopyInternal( original );
        @SuppressWarnings( "unchecked" )
        final T castedRetval = (T)retval;
        return castedRetval;
    }
    
    
    private static Object getDefensiveCopyInternal( final Object original )
    {
        if ( null == original )
        {
            return original;
        }
        if ( Set.class.isAssignableFrom( original.getClass() ) )
        {
            if ( TreeSet.class.isAssignableFrom( original.getClass() ) )
            {
                @SuppressWarnings( "unchecked" )
                final TreeSet< Object > castedOriginal = (TreeSet< Object >)original;
                final TreeSet< Object > retval = new TreeSet<>( castedOriginal.comparator() );
                retval.addAll( castedOriginal );
                return retval;
            }
            final Set< ? > originalSet = (Set< ? >)original;
            return new HashSet<>( originalSet );
        }
        if ( List.class.isAssignableFrom( original.getClass() ) )
        {
            final List< ? > originalList = (List< ? >)original;
            return new ArrayList<>( originalList );
        }
        if ( Map.class.isAssignableFrom( original.getClass() ) )
        {
            final Map< ?, ? > originalMap = (Map< ?, ? >)original;
            return new HashMap<>( originalMap );
        }
        if ( original.getClass().isArray() )
        {
            if ( byte.class == original.getClass().getComponentType() )
            {
                return ( (byte[])original ).clone();
            }
            if ( long.class == original.getClass().getComponentType() )
            {
                return ( (long[])original ).clone();
            }
            final Object [] originalArray = (Object[]) original;
            return originalArray.clone();
        }
        
        return original;
    }
}
