/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.codegen;

import java.util.Date;
import java.util.Set;

import com.spectralogic.util.db.lang.DatabaseNamingConvention;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.DatabaseUtils;
import com.spectralogic.util.db.lang.DatabaseView;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Platform;

public final class CMacroCodeGenerator extends BaseCodeGenerator
{
    public CMacroCodeGenerator( final Set< Class< ? extends DatabasePersistable > > dataTypes )
    {
        super( dataTypes, getFileComments() );
        runGenerator();
    }
    
    
    private static String getFileComments()
    {
        final StringBuilder retval = new StringBuilder();
        retval.append( "// Auto-generated" );
        retval.append( Platform.NEWLINE ).append( "//    By: " + CMacroCodeGenerator.class.getName() );
        retval.append( Platform.NEWLINE ).append( "//    On: " + new Date() );
        return retval.toString();
    }


    @Override
    protected void generateForDomain( final Class< ? > clazz )
    {
        final String headerFileName = NamingConventionType.UNDERSCORED.convert(
                clazz.getSimpleName() + ".h" );
        m_generatedResults.withoutSpacingAdd( 
                MASTER_HEADER_FILE_NAME, "#include <" + headerFileName + ">" );
        
        final StringBuilder code = new StringBuilder();
        code.append( "// " + clazz.getName() );
        code.append( Platform.NEWLINE );
        code.append( Platform.NEWLINE );
        code.append( "#ifndef " + NamingConventionType.CONSTANT.convert( clazz.getSimpleName() ) );
        code.append( Platform.NEWLINE );
        code.append( "#define " + NamingConventionType.CONSTANT.convert( clazz.getSimpleName() ) 
                     + " \"" + DatabaseNamingConvention.toDatabaseTableName( clazz ) + "\"" );
        code.append( Platform.NEWLINE );
        
        final int leftLength = clazz.getSimpleName().length() + 22;
        for ( final String prop : sort( DatabaseUtils.getPersistablePropertyNames( clazz ) ) )
        {
            final StringBuilder line = new StringBuilder();
            line.append( "#define " + NamingConventionType.CONSTANT.convert( clazz.getSimpleName() ) 
                         + "__" + NamingConventionType.CONSTANT.convert( prop ) );
            addWhitespace( line, leftLength );
            line.append( " \"" + DatabaseNamingConvention.toDatabaseColumnName( prop ) + "\"" );
            code.append( Platform.NEWLINE + line.toString() );
        }
        code.append( Platform.NEWLINE ).append( "#endif" );
        
        m_generatedResults.add( headerFileName, code.toString() );
    }


    @Override
    protected void generateForEnum( final Class< ? > enumType )
    {
        final String headerFileName = NamingConventionType.UNDERSCORED.convert(
                enumType.getSimpleName() + ".h" );
        m_generatedResults.withoutSpacingAdd( 
                MASTER_HEADER_FILE_NAME, "#include <" + headerFileName + ">" );

        final int leftLength = enumType.getSimpleName().length() + 22;
        final StringBuilder code = new StringBuilder();
        code.append( "// " + enumType.getName() );
        code.append( Platform.NEWLINE );
        code.append( Platform.NEWLINE );
        code.append( "#ifndef " + NamingConventionType.CONSTANT.convert( enumType.getSimpleName() ) );
        code.append( Platform.NEWLINE );
        code.append( "#define " + NamingConventionType.CONSTANT.convert( enumType.getSimpleName() ) 
                     + " \"" + DatabaseNamingConvention.toDatabaseEnumName( enumType ) + "\"" );
        code.append( Platform.NEWLINE );
        
        for ( final Object c : enumType.getEnumConstants() )
        {
            final StringBuilder line = new StringBuilder();
            line.append( "#define " + NamingConventionType.CONSTANT.convert( enumType.getSimpleName() ) 
                         + "__" + NamingConventionType.CONSTANT.convert( c.toString() ) );
            addWhitespace( line, leftLength );
            line.append( " \"" + NamingConventionType.CONSTANT.convert( c.toString() ) + "\"" );
            code.append( Platform.NEWLINE + line.toString() );
        }
        code.append( Platform.NEWLINE ).append( "#endif" );
        
        m_generatedResults.add( headerFileName, code.toString() );
    }


    @Override
    protected void generateForSchema( final String schemaName )
    {
        // empty
    }


    @Override
    protected void codeGenerationCompleted( final Set< String > schemas )
    {
        // empty
    }

    @Override
    protected void generateForView(Class<? extends DatabaseView> viewClass, String sql) {
        final String headerFileName = NamingConventionType.UNDERSCORED.convert(
                viewClass.getSimpleName() + ".h" );
        m_generatedResults.withoutSpacingAdd( 
                MASTER_HEADER_FILE_NAME, "#include <" + headerFileName + ">" );
        
        final StringBuilder code = new StringBuilder();
        code.append( "// " + viewClass.getName() + " (VIEW)" );
        code.append( Platform.NEWLINE );
        code.append( Platform.NEWLINE );
        code.append( "#ifndef " + NamingConventionType.CONSTANT.convert( viewClass.getSimpleName() ) );
        code.append( Platform.NEWLINE );
        code.append( "#define " + NamingConventionType.CONSTANT.convert( viewClass.getSimpleName() ) 
                     + " \"" + DatabaseNamingConvention.toDatabaseTableName( viewClass ) + "\"" );
        code.append( Platform.NEWLINE ).append( "#endif" );
        
        m_generatedResults.add( headerFileName, code.toString() );
    }


    private final static String MASTER_HEADER_FILE_NAME = "_include_all.h";
}
