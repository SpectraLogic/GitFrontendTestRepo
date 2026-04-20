/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang;

import java.util.HashSet;
import java.util.Set;

public enum NamingConventionType
{
    /**
     * For example, 'somename'
     */
    CONCAT_LOWERCASE(
            new NameConverter()
            {
                public String convert( final String original )
                {
                    return NamingConvention.toConcatenatedLowercase( original );
                }
            },
            "concat" ),

    /**
     * For example, 'SOME_NAME'
     */
    CONSTANT(
            new NameConverter()
            {
                public String convert( final String original )
                {
                    return NamingConvention.toConstantNamingConvention( original );
                }
            },
            "constant" ),

    /**
     * For example, 'some_name'
     */
    UNDERSCORED(
            new NameConverter()
            {
                public String convert( final String original )
                {
                    return NamingConvention.toUnderscoredNamingConvention( original );
                }
            },
            "c", "underscored", "lowercase", "underscore" ),

    /**
     * For example, 'SomeName'
     */
    CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE(
            new NameConverter()
            {
                public String convert( final String original )
                {
                    return NamingConvention.toCamelCaseNamingConventionWithFirstLetterUppercase( original );
                }
            },
            "javaType", "camelCase", "s3", "amazon" ),

    /**
     * For example, 'someName'
     */
    CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE(
            new NameConverter()
            {
                public String convert( final String original )
                {
                    return NamingConvention.toCamelCaseNamingConventionWithFirstLetterLowercase( original );
                }
            },
            "javabean", "camelcaselower" ),
    ;
    
    
    private NamingConventionType( final NameConverter nameConverter, final String ... identifiableBy )
    {
        m_converter = nameConverter;
        m_identifiableBy = new HashSet<>();
        m_identifiableBy.add( NamingConvention.toConcatenatedLowercase( name() ) );
        for ( final String ib : identifiableBy )
        {
            m_identifiableBy.add( NamingConvention.toConcatenatedLowercase( ib ) );
        }
    }
    
    
    public String convert( final String original )
    {
        return m_converter.convert( original );
    }
    
    
    public static NamingConventionType from( String identifier )
    {
        identifier = NamingConventionType.CONCAT_LOWERCASE.convert( identifier );
        for ( final NamingConventionType nc : NamingConventionType.values() )
        {
            if ( nc.m_identifiableBy.contains( identifier ) )
            {
                return nc;
            }
        }
        throw new UnsupportedOperationException( 
                "Could not determine naming convention for: " + identifier );
    }
    
    
    private interface NameConverter
    {
        String convert( final String original );
    }
    
    
    private final NameConverter m_converter;
    private final Set< String > m_identifiableBy;
}
