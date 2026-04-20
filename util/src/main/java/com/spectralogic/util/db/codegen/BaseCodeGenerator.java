/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.codegen;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.lang.*;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.comparator.ClassComparator;

abstract class BaseCodeGenerator
{
    protected BaseCodeGenerator( 
            final Set< Class< ? extends DatabasePersistable > > dataTypes,
            final String codeGeneratorCommentsForFile )
    {
        m_dataTypes = dataTypes;
        Validations.verifyNotNull( "Types", m_dataTypes );
        m_generatedResults = new GeneratedResults( codeGeneratorCommentsForFile );
    }
    
    
    final protected void runGenerator()
    {
        if ( m_generated )
        {
            throw new IllegalStateException( "Already generated code." );
        }
        
        final Map< Class< ? >, Set< Class< ? > > > dependencies = new HashMap<>();
        for ( final Class< ? > clazz : m_dataTypes )
        {
            if ( ReadOnly.class.isAssignableFrom( clazz ) )
            {
                continue;
            }
            
            dependencies.put( clazz, new HashSet< Class< ? > >() );
            for ( final String prop : sort( DatabaseUtils.getPersistablePropertyNames( clazz ) ) )
            {
                final Method reader = BeanUtils.getReader( clazz, prop );
                if ( null == reader )
                {
                    continue;
                }
                
                final References references = reader.getAnnotation( References.class );
                if ( null == references )
                {
                    continue;
                }
                
                final Class< ? > dependency = references.value();
                if ( null == dependency )
                {
                    throw new RuntimeException( "Dependency cannot be null." );
                }
                if ( !m_dataTypes.contains( dependency ) )
                {
                    throw new RuntimeException(
                     "Dependency not included in package to search for db domains: " + dependency.getName() );
                }
                if ( clazz != dependency )
                {
                    dependencies.get( clazz ).add( references.value() );
                }
            }
        }
        
        while ( !dependencies.isEmpty() )
        {
            generateDatabaseCreationSqlScript(
                    0,
                    getNextClass( dependencies.keySet() ), 
                    dependencies );
        }
        generateViews();
        codeGenerationCompleted( m_processedSchemas );
        m_generated = true;
    }
    
    
    private Class< ? > getNextClass( final Set< Class< ? > > classes )
    {
        final List< Class< ? > > sortedClasses = new ArrayList<>( classes );
        Collections.sort( sortedClasses, new ClassComparator() );
        return sortedClasses.get( 0 );
    }
    
    
    private void generateDatabaseCreationSqlScript( 
            final int recursiveCallDepth,
            final Class< ? > clazz,
            final Map< Class< ? >, Set< Class< ? > > > dependencies )
    {
        if ( 100 < recursiveCallDepth )
        {
            throw new RuntimeException( 
                    "Circular dependency detected.  "
                    + "You cannot have table A depend on table B which depends on table A.  Dependencies: "
                    + dependencies );
        }
        
        if ( !dependencies.get( clazz ).isEmpty() )
        {
            generateDatabaseCreationSqlScript(
                    recursiveCallDepth + 1,
                    getNextClass( dependencies.get( clazz ) ),
                    dependencies );
            return;
        }

        generateSchema( clazz );
        dependencies.remove( clazz );
        for ( final Map.Entry< Class< ? >, Set< Class< ? > > > e : dependencies.entrySet() )
        {
            e.getValue().remove( clazz );
        }
        
        try
        {
            for ( final String prop : sort( DatabaseUtils.getPersistablePropertyNames( clazz ) ) )
            {
                final Method reader = BeanUtils.getReader( clazz, prop );
                final Method writer = BeanUtils.getWriter( clazz, prop );
                if ( null == reader || null == writer )
                {
                    throw new RuntimeException(
                            "You must define both a reader and writer for: " + prop );
                }
                
                if ( reader.getReturnType().isEnum() )
                {
                    if ( !m_processedEnums.contains( reader.getReturnType() ) )
                    {
                        generateSchema( reader.getReturnType() );
                        m_processedEnums.add( reader.getReturnType() );
                        generateForEnum( reader.getReturnType() );
                    }
                }
            }
            
            generateForDomain( clazz );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( "Failed to generate SQL code for: " + clazz, ex );
        }
    }
    
    
    private void generateSchema( final Class< ? > type )
    {
        final String schemaName = DatabaseNamingConvention.getSchemaName( type );
        if ( m_processedSchemas.contains( schemaName ) )
        {
            return;
        }
        
        m_processedSchemas.add( schemaName );
        generateForSchema( schemaName );
    }
    
    
    protected abstract void generateForDomain( final Class< ? > clazz );
    
    
    protected abstract void generateForEnum( final Class< ? > enumType );
    
    
    protected abstract void generateForSchema( final String schemaName );
    
    
    protected abstract void codeGenerationCompleted( final Set< String > schemas );
    
    
    protected abstract void generateForView( final Class< ? extends DatabaseView > viewClass, final String sql );
    
    
    private void generateViews()
    {
        final Map< String, Class< ? extends DatabaseView > > viewsByName = new HashMap<>();

        for ( final Class< ? > clazz : m_dataTypes )
        {
            if ( DatabaseView.class.isAssignableFrom( clazz ) )
            {
                final ViewDefinition viewDef = clazz.getAnnotation( ViewDefinition.class );
                if ( viewDef != null )
                {
                    @SuppressWarnings( "unchecked" )
                    final Class< ? extends DatabaseView > viewClass = ( Class< ? extends DatabaseView > ) clazz;
                    viewsByName.put( viewClass.getName(), viewClass );
                }
            }
        }

        for ( final String viewName : sort( viewsByName.keySet() ) )
        {
            final Class< ? extends DatabaseView > viewClass = viewsByName.get( viewName );
            final ViewDefinition viewDef = viewClass.getAnnotation( ViewDefinition.class );
            generateForView( viewClass, viewDef.value() );
        }
    }
    
    
    protected static void addWhitespace( final StringBuilder sb, final int desiredNewLength )
    {
        for ( int i = sb.length(); i <= desiredNewLength; ++i )
        {
            sb.append( ' ' );
        }
    }
    
    
    protected static List< String > sort( final Set< String > set )
    {
        final List< String > retval = new ArrayList<>( set );
        Collections.sort( retval );
        return retval;
    }
    
    
    final public GeneratedResults getGeneratedCode()
    {
        if ( !m_generated )
        {
            throw new IllegalStateException( "Code generator must generate code first." );
        }
        
        return m_generatedResults;
    }
    
    
    private final Set< Class< ? extends DatabasePersistable > > m_dataTypes;
    private final Set< Class< ? > > m_processedEnums = new HashSet<>();
    private final Set< String > m_processedSchemas = new HashSet<>();
    protected final GeneratedResults m_generatedResults;
    private volatile boolean m_generated;
}
