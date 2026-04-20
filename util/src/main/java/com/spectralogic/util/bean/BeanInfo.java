/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.bean;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.spectralogic.util.bean.BeanComparator.BeanPropertyComparisonSpecifiction;
import com.spectralogic.util.bean.BeanSQLOrdering.BeanSQLOrderingSpecification;
import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.DefaultDoubleValue;
import com.spectralogic.util.bean.lang.DefaultEnumValue;
import com.spectralogic.util.bean.lang.DefaultIntegerValue;
import com.spectralogic.util.bean.lang.DefaultLongValue;
import com.spectralogic.util.bean.lang.DefaultStringValue;
import com.spectralogic.util.bean.lang.DefaultToCurrentDate;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.bean.lang.SortBy.Direction;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;
import com.spectralogic.util.db.lang.shared.ExcludeFromDatabasePersistence;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.MockObjectFactory;

final class BeanInfo
{
    BeanInfo( final Class< ? > clazz, final BeanInfoCache beanInfoCache )
    {
        Validations.verifyNotNull( "Class", clazz );
        Validations.verifyNotNull( "Bean info cache", beanInfoCache );
        
        m_clazz = clazz;
        if ( null != clazz.getSuperclass() )
        {
            addAll( beanInfoCache.getBeanInfo( clazz.getSuperclass() ) );
        }
        for ( final Class< ? > c : clazz.getInterfaces() )
        {
            addAll( beanInfoCache.getBeanInfo( c ) );
        }
        
        for ( final Method m : clazz.getDeclaredMethods() )
        {
            if ( m.isSynthetic() )
            {
                continue;
            }
            final String propertyName = getBeanPropertyNameForMethod( m );
            if ( null == propertyName || NON_PROPERTY_NAMES.contains( propertyName ) )
            {
                continue;
            }
            
            if ( m.getName().startsWith( "get" )
                    || ( m.getName().startsWith( "is" ) && boolean.class == m.getReturnType() && !m.getName().equals("isLatin1") ) )
            {
                if ( void.class != m.getReturnType() && !Modifier.isStatic( m.getModifiers() ) )
                {
                    m.setAccessible( true );
                    m_propNameToReaderMethodMap.put( propertyName, m );
                    m_readerMethodToPropNameMap.put( m, propertyName );
                    final DefaultBeanPropertyValueProvider defaultValueProvider =
                            getDefaultBeanPropValueProvider( m );
                    if ( null != defaultValueProvider )
                    {
                        m_defaultPropValueMap.put( propertyName, defaultValueProvider );
                    }
                }
            }
            
            if ( m.getName().startsWith( "set" ) 
                    && 1 == m.getParameterTypes().length
                    && !Modifier.isStatic( m.getModifiers() ) )
            {
                m.setAccessible( true );
                m_propNameToWriterMethodMap.put( propertyName, m );
                m_writerMethodToPropNameMap.put( m, propertyName );
            }
        }
    }
    
    
    private DefaultBeanPropertyValueProvider getDefaultBeanPropValueProvider( final Method reader )
    {
        final Class< ? > type = reader.getReturnType();
        
        final DefaultIntegerValue defaultIntegerValue = reader.getAnnotation( DefaultIntegerValue.class );
        if ( null != defaultIntegerValue && !( int.class == type || Integer.class == type ) )
        {
            throw new RuntimeException(
                    reader + " has illegal default value annotations." );
        }
        
        final DefaultDoubleValue defaultDoubleValue = reader.getAnnotation( DefaultDoubleValue.class );
        if ( null != defaultDoubleValue && !( double.class == type || Double.class == type ) )
        {
            throw new RuntimeException(
                    reader + " has illegal default value annotations." );
        }

        final DefaultLongValue defaultLongValue = reader.getAnnotation( DefaultLongValue.class );
        if ( null != defaultLongValue && !( long.class == type || Long.class == type ) )
        {
            throw new RuntimeException(
                    reader + " has illegal default value annotations." );
        }
        
        final DefaultBooleanValue defaultBooleanValue = reader.getAnnotation( DefaultBooleanValue.class );
        if ( null != defaultBooleanValue && !( boolean.class == type || Boolean.class == type ) )
        {
            throw new RuntimeException(
                    reader + " has illegal default value annotations." );
        }
        
        final DefaultToCurrentDate defaultToCurrentDate = reader.getAnnotation( DefaultToCurrentDate.class );
        if ( null != defaultToCurrentDate && Date.class != type )
        {
            throw new RuntimeException(
                    reader + " has illegal default value annotations." );
        }
        
        final DefaultStringValue defaultStringValue = reader.getAnnotation( DefaultStringValue.class );
        if ( null != defaultStringValue && String.class != type )
        {
            throw new RuntimeException(
                    reader + " has illegal default value annotations." );
        }
        
        final DefaultEnumValue defaultEnumValue = reader.getAnnotation( DefaultEnumValue.class );
        if ( null != defaultEnumValue && !type.isEnum() )
        {
            throw new RuntimeException(
                    reader + " has illegal default value annotations." );
        }
        
        if ( null != defaultIntegerValue )
        {
            return new ConstantDefaultBeanPropertyValueProvider( 
                    Integer.valueOf( defaultIntegerValue.value() ) );
        }
        if ( null != defaultDoubleValue )
        {
            return new ConstantDefaultBeanPropertyValueProvider( 
                    Double.valueOf( defaultDoubleValue.value() ) );
        }
        if ( null != defaultLongValue )
        {
            return new ConstantDefaultBeanPropertyValueProvider( 
                    Long.valueOf( defaultLongValue.value() ) );
        }
        if ( null != defaultBooleanValue )
        {
            return new ConstantDefaultBeanPropertyValueProvider( 
                    Boolean.valueOf( defaultBooleanValue.value() ) );
        }
        if ( null != defaultToCurrentDate )
        {
            return new CurrentDateDefaultBeanPropertyValueProvider(); 
        }
        if ( null != defaultStringValue )
        {
            return new ConstantDefaultBeanPropertyValueProvider( defaultStringValue.value() );
        }
        if ( null != defaultEnumValue )
        {
            return new ConstantDefaultBeanPropertyValueProvider( 
                    ReflectUtil.enumValueOf( type, defaultEnumValue.value() ) );
        }
        if ( type.isPrimitive() )
        {
            return new ConstantDefaultBeanPropertyValueProvider(
                    MockObjectFactory.objectForType( type ) );
        }
        return null;
    }
    
    
    private void addAll( final BeanInfo beanInfo )
    {
        if ( null == beanInfo )
        {
            return;
        }
        
        m_propNameToReaderMethodMap.putAll( beanInfo.getPropNameToReaderMethodMap() );
        m_propNameToWriterMethodMap.putAll( beanInfo.getPropNameToWriterMethodMap() );
        m_defaultPropValueMap.putAll( beanInfo.getDefaultPropValueMap() );
        m_readerMethodToPropNameMap.putAll( beanInfo.getReaderMethodToPropNameMap() );
        m_writerMethodToPropNameMap.putAll( beanInfo.getWriterMethodToPropNameMap() );
        m_columnIndexes.addAll( beanInfo.m_columnIndexes );
    }
    
    
    /**
     * <font color = red>Map returned is not defensively copied - DO NOT MODIFY IT!</font>
     */
    Map< String, Method > getPropNameToReaderMethodMap()
    {
        return m_propNameToReaderMethodMap;
    }
    
    
    /**
     * <font color = red>Map returned is not defensively copied - DO NOT MODIFY IT!</font>
     */
    Map< String, Method > getPropNameToWriterMethodMap()
    {
        return m_propNameToWriterMethodMap;
    }
    
    
    /**
     * <font color = red>Map returned is not defensively copied - DO NOT MODIFY IT!</font>
     * @return Map< Method, String >
     */
    Map< Method, String > getReaderMethodToPropNameMap()
    {
        return m_readerMethodToPropNameMap;
    }
    
    
    /**
     * <font color = red>Map returned is not defensively copied - DO NOT MODIFY IT!</font>
     */
    Map< Method, String > getWriterMethodToPropNameMap()
    {
        return m_writerMethodToPropNameMap;
    }


    /**
     * <font color = red>Map returned is not defensively copied - DO NOT MODIFY IT!</font>
     */
    Map< String, DefaultBeanPropertyValueProvider > getDefaultPropValueMap()
    {
        return m_defaultPropValueMap;
    }
    
    
    synchronized Comparator< Object > getComparator()
    {
        if ( !m_computedComparator )
        {
            computeComparator();
            m_computedComparator = true;
        }
        return m_comparator;
    }
    
    
    synchronized Set< String > getColumnIndexes()
    {
        if ( !m_retrievedIndexes )
        {
            parseAnnotationIndexes();
            getComparator();
            validateSortByWithIndexes();
            m_retrievedIndexes = true;
        }
        return m_columnIndexes;
    }
    
    
    BeanSQLOrdering getBeanSqlOrdering()
    {
        getComparator();
        return m_sqlOrdering;
    }
    
    
    private void parseAnnotationIndexes()
    {
        final UniqueIndexes uniqueIndexesAnnotation = m_clazz.getAnnotation( UniqueIndexes.class );
        if ( uniqueIndexesAnnotation != null )
        {
            for ( final Unique uniqueIndex : uniqueIndexesAnnotation.value() )
            {
                m_columnIndexes.addAll( CollectionFactory.toList( uniqueIndex.value() ) );
            }
        }
        final Indexes indexesAnnotation = m_clazz.getAnnotation( Indexes.class );
        if ( indexesAnnotation != null )
        {
            for ( final Index index : indexesAnnotation.value() )
            {
                m_columnIndexes.addAll( CollectionFactory.toList( index.value() ) );
            }
        }
    }
    
    
    private void validateSortByWithIndexes()
    {
        for ( String order : m_sqlOrdering.getSortColumnNames() )
        {
            if ( !m_columnIndexes.contains( order ) )
            {
                throw new UnsupportedOperationException( "Tried to sort by " + order + " in " + m_clazz.getName() +
                        " via annotations but there is no column index with those properties in them." );
            }
        }
    }
    
    
    private void computeComparator()
    {
        final Map< Integer, String > sortProperties = new HashMap<>();
        final Map< Integer, Direction > sortDirections = new HashMap<>();
        Set< Integer > nonPersistedProperty = new HashSet<>();
        for ( final Map.Entry< Method, String > e : getReaderMethodToPropNameMap().entrySet() )
        {
            final Method reader = e.getKey();
            final String property = e.getValue();
            final SortBy sortBy = reader.getAnnotation( SortBy.class );
            if ( null != sortBy )
            {
                final Integer precedence = Integer.valueOf( sortBy.value() );
                if ( sortProperties.containsKey( precedence ) )
                {
                    throw new RuntimeException(
                            "You cannot have multiple " + SortBy.class.getName()
                            + " annotations on different properties with the same precedence." );
                }
                sortProperties.put( precedence, property );
                sortDirections.put( precedence, sortBy.direction() );
                if ( null != reader.getAnnotation( ExcludeFromDatabasePersistence.class ) )
                {
                    nonPersistedProperty.add( precedence );
                }
            }
        }
        
        final List< Integer > precedences = new ArrayList<>( sortProperties.keySet() );
        Collections.sort( precedences );
        final List< BeanPropertyComparisonSpecifiction > sortSpecs = new ArrayList<>();
        final List< BeanSQLOrderingSpecification > sortSpecsSql = new ArrayList<>();
        for ( final Integer precedence : precedences )
        {
            sortSpecs.add( new BeanPropertyComparisonSpecifiction(
                    sortProperties.get( precedence ),
                    sortDirections.get( precedence ),
                    null ) );
            if ( !nonPersistedProperty.contains( precedence ) )
            {
                sortSpecsSql.add( new BeanSQLOrderingSpecification( sortProperties.get( precedence ),
                        sortDirections.get( precedence ) ) );
            }
        }
        if ( !sortSpecs.isEmpty() )
        {
            if ( !getPropNameToReaderMethodMap().containsKey( Identifiable.ID ) )
            {
                throw new UnsupportedOperationException(
                        "In order to ensure that 2 bean instances never have a zero compare result, " 
                        + "we must have a property that uniquely identifies a bean as a fall-back.  " 
                        + "Failure to have such a property means that some types like "
                        + TreeSet.class.getSimpleName() + "will fail to contain 2 different bean instances.  "
                        + "A property that uniquely identifies a bean could not be found on " 
                        + m_clazz.getName() + "." );
            }
            sortSpecs.add( new BeanPropertyComparisonSpecifiction(
                    Identifiable.ID,
                    Direction.ASCENDING, 
                    null ) );
        }
        
        @SuppressWarnings( "unchecked" )
        final Class< Object > castedClass = (Class< Object >)m_clazz;
        m_comparator = ( sortSpecs.isEmpty() ) ?
                null
                : new BeanComparator<>( 
                        castedClass,
                        CollectionFactory.toArray( BeanPropertyComparisonSpecifiction.class, sortSpecs ) );
        m_sqlOrdering =
                new BeanSQLOrdering( CollectionFactory.toArray( BeanSQLOrderingSpecification.class, sortSpecsSql ) );
    }
    
    
    static String getBeanPropertyNameForMethod( final Method method )
    {
        Validations.verifyNotNull( "Method", method );
        if ( method.getName().startsWith( "get" ) || method.getName().startsWith( "set" ) )
        {
            return method.getName().substring( 3, 4 ).toLowerCase() + method.getName().substring( 4 );
        }
        if ( method.getName().startsWith( "is" ) )
        {
            return method.getName().substring( 2, 3 ).toLowerCase() + method.getName().substring( 3 );
        }
        return null;
    }
    
    
    private final Map< String, Method > m_propNameToReaderMethodMap = new HashMap<>();
    private final Map< String, Method > m_propNameToWriterMethodMap = new HashMap<>();
    private final Map< String, DefaultBeanPropertyValueProvider > m_defaultPropValueMap = new HashMap<>();
    private final Map< Method, String > m_readerMethodToPropNameMap = new HashMap<>();
    private final Map< Method, String > m_writerMethodToPropNameMap = new HashMap<>();
    private final Class< ? > m_clazz;
    
    private final Set< String > m_columnIndexes = CollectionFactory.toSet( Identifiable.ID );
    private boolean m_retrievedIndexes;
    
    private BeanSQLOrdering m_sqlOrdering;
    private Comparator< Object > m_comparator;
    private boolean m_computedComparator;
    
    private final static Set< String > NON_PROPERTY_NAMES;
    static
    {
        NON_PROPERTY_NAMES = new HashSet<>();
        NON_PROPERTY_NAMES.add( "bytes" );
        NON_PROPERTY_NAMES.add( "class" );
        NON_PROPERTY_NAMES.add( "proxyClass" );
        NON_PROPERTY_NAMES.add( "proxyClass0" );
        NON_PROPERTY_NAMES.add( "invocationHandler" );
    }
}
