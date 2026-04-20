/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.codegen;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.AutoIncrementing;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
import com.spectralogic.util.db.lang.DatabaseNamingConvention;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.DatabaseUtils;
import com.spectralogic.util.db.lang.DatabaseView;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;
import com.spectralogic.util.db.lang.MustMatchRegularExpression;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.reflect.ReflectUtil;

public final class SqlCodeGenerator extends BaseCodeGenerator
{
    public SqlCodeGenerator(
            final Set< Class< ? extends DatabasePersistable > > dataTypes,
            final String dbUserNameToGrantAccessTo )
    {
        super( dataTypes, getFileComments() );
        m_dbUserNameToGrantAccessTo = dbUserNameToGrantAccessTo;
        Validations.verifyNotNull( "Db user to grant access to", m_dbUserNameToGrantAccessTo );
        
        runGenerator();
    }
    
    
    private static String getFileComments()
    {
        final StringBuilder retval = new StringBuilder();
        retval.append( "-- Auto-generated" );
        retval.append( Platform.NEWLINE ).append( "--    By: " + SqlCodeGenerator.class.getName() );
        retval.append( Platform.NEWLINE ).append( "--    On: " + new Date() );
        return retval.toString();
    }
    
    
    @Override
    protected void generateForEnum( final Class< ? > enumType )
    {
        final StringBuilder sql = new StringBuilder();
        sql.append( "-- " + enumType.getName() + " --" );
        sql.append( Platform.NEWLINE );
        sql.append( "CREATE TYPE " 
                       + DatabaseNamingConvention.toDatabaseEnumName( enumType ) + " AS ENUM ();" );
        
        m_generatedResults.add( null, sql.toString() );
    }


    @Override
    protected void generateForSchema( final String schemaName )
    {
        final StringBuilder generatedCode = new StringBuilder();
        generatedCode.append( "-- SCHEMA " + schemaName + " --" );
        generatedCode.append( Platform.NEWLINE );
        generatedCode.append( "DO $$" );
        generatedCode.append( Platform.NEWLINE );
        generatedCode.append( "BEGIN" );
        generatedCode.append( Platform.NEWLINE );
        generatedCode.append( "IF NOT EXISTS (SELECT * FROM information_schema.schemata WHERE schema_name = '"
                              + schemaName + "') THEN" );
        generatedCode.append( Platform.NEWLINE );
        generatedCode.append( "  CREATE SCHEMA " + schemaName + ";" );
        generatedCode.append( Platform.NEWLINE );
        generatedCode.append( "END IF;" );
        generatedCode.append( Platform.NEWLINE );
        generatedCode.append( "END" );
        generatedCode.append( Platform.NEWLINE );
        generatedCode.append( "$$;" );
        generatedCode.append( Platform.NEWLINE );
        
        m_generatedResults.add( null, generatedCode.toString() );
    }

    
    @Override
    protected void codeGenerationCompleted( final Set< String > schemas )
    {
        // empty
    }
    
    
    @Override
    protected void generateForDomain( final Class< ? > clazz )
    {
        final String databaseTableName = DatabaseNamingConvention.toDatabaseTableName( clazz );
        final String [] dbNameComponents = databaseTableName.split( Pattern.quote( "." ) );
        final StringBuilder sql = new StringBuilder();
        sql.append( "-- " + clazz.getName() +  " --" );
        sql.append( Platform.NEWLINE );
        sql.append( "DO $$" );
        sql.append( Platform.NEWLINE );
        sql.append( "BEGIN" );
        sql.append( Platform.NEWLINE );
        sql.append( "IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='" );
        sql.append( dbNameComponents[ 0 ] );
        sql.append( "' AND tablename='" );
        sql.append( dbNameComponents[ 1 ] );
        sql.append( "') THEN" );
        sql.append( Platform.NEWLINE );
        sql.append( "  CREATE TABLE " ).append( databaseTableName ).append( " (" );
        final List< String > primaryKeys = new ArrayList<>();
        final List< String > foreignKeyColumns = new ArrayList<>();
        for ( final String prop : sort( DatabaseUtils.getPersistablePropertyNames( clazz ) ) )
        {
            final Method reader = BeanUtils.getReader( clazz, prop );
            final Method writer = BeanUtils.getWriter( clazz, prop );

            verifyAnnotationsNotOnWriter( writer );
            
            final boolean optional = ( null != reader.getAnnotation( Optional.class ) );
            final boolean autoIncrementing = ( null != reader.getAnnotation( AutoIncrementing.class ) );
            if ( null != reader.getAnnotation( References.class ) )
            {
                foreignKeyColumns.add( prop );
            }
            
            if ( (optional || autoIncrementing) && reader.getReturnType().isPrimitive() )
            {
                throw new RuntimeException( 
                        "For " + prop + ", you cannot use a primitive type that is " 
                         + "optional or auto-incrementing in the database. " +
                                " Please use the object type instead." );
            }
            if ( !optional && !autoIncrementing
                    && !reader.getReturnType().isPrimitive() 
                    && ReflectUtil.toPrimitiveType( reader.getReturnType() ).isPrimitive() )
            {
                throw new RuntimeException( 
                        "For " + prop + ", you cannot use a non-primitive type that is " 
                         + "required in the database.  Please use the primitive type instead." );
            }
            
            final MustMatchRegularExpression regex = reader.getAnnotation( MustMatchRegularExpression.class );
            if ( null != regex )
            {
                if ( String.class != reader.getReturnType() )
                {
                    throw new RuntimeException( 
                            MustMatchRegularExpression.class.getSimpleName()
                            + " can only be used on string properties: " + reader );
                }
                Pattern.compile( regex.value() );
            }
            
            final String eColumnName = DatabaseNamingConvention.toDatabaseColumnName( prop ) ;
            final String eType = DatabaseUtils.toDatabaseType( reader.getReturnType(), autoIncrementing);
            final String eNullness = optional ? " NULL" : " NOT NULL";
            final String eReferences = getReferences( reader );
            final String eCascadeDelete = getCascadeDelete( reader );
            
            final StringBuilder line = new StringBuilder();
            line.append( Platform.NEWLINE ).append( "    " ).append( eColumnName ).append( " " );
            addWhitespace( line, 22 );
            line.append( eType );
            addWhitespace( line, 44 );
            line.append( eNullness );
            addWhitespace( line, 55 );
            line.append( eReferences );
            addWhitespace( line, 65 );
            line.append( eCascadeDelete );
            sql.append( line );
            
            while ( ' ' == sql.charAt( sql.length() - 1 ) )
            {
                sql.deleteCharAt( sql.length() - 1 );
            }
            sql.append( "," );
            
            if ( prop.equals( DatabaseUtils.getPrimaryKeyPropertyName( clazz ) ) )
            {
                if ( optional )
                {
                    throw new RuntimeException( prop + " cannot be optional and a primary key." );
                }
                primaryKeys.add( prop );
            }
        }
        
        if ( primaryKeys.isEmpty() )
        {
            throw new RuntimeException( "At least one primary key must be defined." );
        }
        
        sql.append( getPrimaryKeyDefinition( primaryKeys ) );

        final Set< List< String >> uniqueIndexes = getUniqueIndexes( clazz );
        for ( final List< String > uniqueIndex : uniqueIndexes )
        {
            appendUniqueIndexDefinition( sql, uniqueIndex );
        }

        sql.append( Platform.NEWLINE ).append( "  );" ).append( Platform.NEWLINE );

        final Set< List< String > > indexes = getIndexes( clazz );
        for ( final String foreignKeyColumn : foreignKeyColumns )
        {
            if ( primaryKeys.contains( foreignKeyColumn ) )
            {
                continue;
            }
            indexes.add( CollectionFactory.toList( foreignKeyColumn ) );
        }
        indexes.removeAll( uniqueIndexes );
        for ( final List< String > index : indexes )
        {
            sql.append( "CREATE INDEX ON " ).append( databaseTableName ).append( " (" );
            boolean isFirst = true;
            for ( final String column : index )
            {
                if ( isFirst )
                {
                    isFirst = false;
                }
                else
                {
                    sql.append( ", " );
                }
                sql.append( DatabaseNamingConvention.toDatabaseColumnName( column ) );
                
            }
            sql.append( ");" ).append( Platform.NEWLINE );
        }

        sql
                .append( "END IF;" )
                .append( Platform.NEWLINE )
                .append( "END" )
                .append( Platform.NEWLINE )
                .append( "$$;" )
                .append( Platform.NEWLINE );
        
        m_generatedResults.add( null, sql.toString() );
    }
    
    
    private void verifyAnnotationsNotOnWriter( final Method writer )
    {
        verifyAnnotationNotOnWriter( MustMatchRegularExpression.class, writer );
        verifyAnnotationNotOnWriter( SortBy.class, writer );
        verifyAnnotationNotOnWriter( References.class, writer );
        verifyAnnotationNotOnWriter( Optional.class, writer );
    }
    
    
    private void verifyAnnotationNotOnWriter( 
            final Class< ? extends Annotation > annotation,
            final Method writer )
    {
        if ( null != writer.getAnnotation( annotation ) )
        {
            throw new RuntimeException(
                    annotation
                    + " annotation must be on the reader, not on the writer.  Writer is in violation: "
                    + writer );
        }
    }  
    
    
    private String getPrimaryKeyDefinition( final List< String > primaryKeys )
    {
        final StringBuilder retval = new StringBuilder();

        retval.append( Platform.NEWLINE ).append( Platform.NEWLINE ).append( "    PRIMARY KEY (" );
        boolean isFirst = true;
        for ( final String primaryKey : primaryKeys )
        {
            if ( isFirst )
            {
                isFirst = false;
            }
            else
            {
                retval.append( ", " );
            }
            retval.append( DatabaseNamingConvention.toDatabaseColumnName( primaryKey ) );
        }
        retval.append( ")" );
        
        return retval.toString();
    }


    private static Set< List< String > > getIndexes( final Class< ? > clazz )
    {
        final Set< List< String > > indexes = new TreeSet<>( StringListComparator.instance() );
        final Indexes indexesAnnotation = clazz.getAnnotation( Indexes.class );
        if ( indexesAnnotation != null )
        {
            for ( final Index index : indexesAnnotation.value() )
            {
                indexes.add( CollectionFactory.toList( index.value() ) );
            }
        }
        return indexes;
    }


    private static Set< List< String > > getUniqueIndexes( final Class< ? > clazz )
    {
        final Set< List< String > > uniqueIndexes = new TreeSet<>( StringListComparator.instance() );
        final UniqueIndexes uniqueIndexesAnnotation = clazz.getAnnotation( UniqueIndexes.class );
        if ( uniqueIndexesAnnotation != null )
        {
            for ( final Unique uniqueIndex : uniqueIndexesAnnotation.value() )
            {
                uniqueIndexes.add( CollectionFactory.toList( uniqueIndex.value() ) );
            }
        }
        return uniqueIndexes;
    }
    
    
    private static void appendUniqueIndexDefinition(
            final StringBuilder stringBuilder,
            final List< String > beanProperties )
    {
        boolean isFirst = true;
        for ( final String prop : beanProperties )
        {
            stringBuilder.append( ", " );
            if ( isFirst )
            {
                isFirst = false;
                stringBuilder
                    .append( Platform.NEWLINE )
                    .append( "    UNIQUE (" );
            }
            stringBuilder.append( DatabaseNamingConvention.toDatabaseColumnName( prop ) );
        }
        stringBuilder.append( ")" );
    }
    
    
    private String getReferences( final Method reader )
    {
        final References references = reader.getAnnotation( References.class );
        if ( null == references )
        {
            return "";
        }
        return " REFERENCES " + DatabaseNamingConvention.toDatabaseTableName( references.value() ) 
               + " ON UPDATE CASCADE";
    }
    
    
    private String getCascadeDelete( final Method reader )
    {
        final References references = reader.getAnnotation( References.class );
        final CascadeDelete cascadeDelete = reader.getAnnotation( CascadeDelete.class );
        if ( null == cascadeDelete )
        {
            return "";
        }
        
        final boolean optional = ( null != reader.getAnnotation( Optional.class ) );
        final WhenReferenceIsDeleted wrid = cascadeDelete.value();
        if ( null == wrid )
        {
            throw new RuntimeException(
                    "On " + reader + ", " + CascadeDelete.class.getSimpleName() + " did not specify " 
                    + WhenReferenceIsDeleted.class.getSimpleName() + "." );
        }
        if ( null == references )
        {
            throw new RuntimeException( 
                    "On " + reader + ", " + CascadeDelete.class.getSimpleName() 
                    + " can only be used in conjunction with " + References.class.getSimpleName() );
        }
        if ( optional && WhenReferenceIsDeleted.DEFAULT == wrid )
        {
            throw new RuntimeException(
                    "On " + reader + ", " + CascadeDelete.class.getSimpleName() 
                    + " did not specify a non-default " + WhenReferenceIsDeleted.class.getSimpleName() 
                    + ", but this is required since the property is optional and infrastructure cannot " 
                    + "'guess' what behavior you want." );
        }
        return " ON DELETE " + wrid.getSql();
    }
    
    
    @Override
    protected void generateForView( final Class< ? extends DatabaseView > viewClass, final String sql )
    {
        final String databaseViewName = DatabaseNamingConvention.toDatabaseTableName( viewClass );
        final String [] dbNameComponents = databaseViewName.split( Pattern.quote( "." ) );
        final StringBuilder generatedSql = new StringBuilder();
        generatedSql.append( "-- " + viewClass.getName() + " (VIEW) --" );
        generatedSql.append( Platform.NEWLINE );
        generatedSql.append( "DO $$" );
        generatedSql.append( Platform.NEWLINE );
        generatedSql.append( "BEGIN" );
        generatedSql.append( Platform.NEWLINE );
        generatedSql.append( "IF NOT EXISTS (SELECT * FROM pg_catalog.pg_views WHERE schemaname='" );
        generatedSql.append( dbNameComponents[ 0 ] );
        generatedSql.append( "' AND viewname='" );
        generatedSql.append( dbNameComponents[ 1 ] );
        generatedSql.append( "') THEN" );
        generatedSql.append( Platform.NEWLINE );
        generatedSql.append( "  CREATE VIEW " ).append( databaseViewName ).append( " AS " );
        generatedSql.append( Platform.NEWLINE );
        generatedSql.append( "  " ).append( sql ).append( ";" );
        generatedSql.append( Platform.NEWLINE );
        generatedSql.append( "END IF;" );
        generatedSql.append( Platform.NEWLINE );
        generatedSql.append( "END" );
        generatedSql.append( Platform.NEWLINE );
        generatedSql.append( "$$;" );
        generatedSql.append( Platform.NEWLINE );
        
        m_generatedResults.add( null, generatedSql.toString() );
    }
    
    
    private final String m_dbUserNameToGrantAccessTo;
}
