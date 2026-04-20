/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang;

import com.spectralogic.util.cache.CacheResultProvider;
import com.spectralogic.util.cache.StaticCache;

public final class NamingConvention
{
    private NamingConvention()
    {
        // singleton
    }
    
    
    /**
     * Given input someName, output is somename
     */
    public static String toConcatenatedLowercase( final String original )
    {
        Validations.verifyNotNull( "Original name", original );
        return CONCATENATED_LOWERCASE_NAMING_CONVENTION_CACHE.get( original );
    }
    

    /**
     * Given input someName, output is SOME_NAME
     */
    public static String toConstantNamingConvention( final String original )
    {
        Validations.verifyNotNull( "Original name", original );
        return CONSTANT_NAMING_CONVENTION_CACHE.get( original );
    }
    

    /**
     * Given input someName, output is some_name
     */
    public static String toUnderscoredNamingConvention( final String original )
    {
        Validations.verifyNotNull( "Original name", original );
        return UNDERSCORE_NAMING_CONVENTION_CACHE.get( original );
    }
    

    /**
     * Given input someName, output is someName
     */
    public static String toCamelCaseNamingConventionWithFirstLetterLowercase( final String original )
    {
        Validations.verifyNotNull( "Original name", original );
        return CAMEL_CASE_NAMING_CONVENTION_CACHE.get( original );
    }
    

    /**
     * Given input someName, output is SomeName
     */
    public static String toCamelCaseNamingConventionWithFirstLetterUppercase( final String original )
    {
        Validations.verifyNotNull( "Original name", original );
        final String retval = CAMEL_CASE_NAMING_CONVENTION_CACHE.get( original );
        if ( retval.isEmpty() )
        {
            return retval;
        }
        return retval.substring( 0, 1 ).toUpperCase() + retval.substring( 1 );
    }
    
    
    private final static class ConstantNamingConventionCacheResultProvider
        implements CacheResultProvider< String, String >
    {
        public String generateCacheResultFor( final String original )
        {
            return toUnderscoredNamingConvention( original ).toUpperCase();
        }
    } // end inner class def
    
    
    private final static class ConcatenatedLowercaseNamingConventionCacheResultProvider
        implements CacheResultProvider< String, String >
    {
        public String generateCacheResultFor( final String original )
        {
            return toUnderscoredNamingConvention( original ).replace( "_", "" );
        }
    } // end inner class def
    
    
    private final static class UnderscoreNamingConventionCacheResultProvider
        implements CacheResultProvider< String, String >
    {
        public String generateCacheResultFor( String original )
        {
            if ( original.isEmpty() )
            {
                return "";
            }
            
            original = original.replace( "-", "_" );
            final StringBuilder retval = new StringBuilder();
            
            boolean someLowercase = false;
            for ( int i = 0; i < original.length(); ++i )
            {
                final char c = original.charAt( i );
                if ( Character.isUpperCase( c ) )
                {
                    retval.append( "_" ).append( Character.toLowerCase( c ) );
                }
                else
                {
                    retval.append( c );
                }
                if ( Character.isLowerCase( c ) )
                {
                    someLowercase = true;
                }
            }
            
            if ( !someLowercase )
            {
                return original.toLowerCase();
            }
            
            while ( '_' == retval.charAt( 0 ) )
            {
                retval.deleteCharAt( 0 );
            }
            return retval.toString();
        }
    } // end inner class def
    
    
    private final static class CamelCaseNamingConventionCacheResultProvider
        implements CacheResultProvider< String, String >
    {
        public String generateCacheResultFor( String original )
        {
            if ( original.isEmpty() )
            {
                return "";
            }

            original = original.replace( "-", "_" );
            final StringBuilder retval = new StringBuilder();
            
            boolean someLowercase = false;
            boolean upperCaseNext = false;
            for ( int i = 0; i < original.length(); ++i )
            {
                final char c = original.charAt( i ) ;
                if ( '_' == c )
                {
                    upperCaseNext = true;
                }
                else
                {
                    if ( upperCaseNext )
                    {
                        retval.append( Character.toUpperCase( c ) );
                        upperCaseNext = false;
                    }
                    else
                    {
                        retval.append( c );
                    }
                }
                if ( Character.isLowerCase( c ) )
                {
                    someLowercase = true;
                }
            }
            
            if ( !someLowercase )
            {
                return generateCacheResultFor( original.toLowerCase() );
            }
            
            return retval.toString().substring( 0, 1 ).toLowerCase() + retval.toString().substring( 1 );
        }
    } // end inner class def
    
    
    private final static StaticCache< String, String > CONCATENATED_LOWERCASE_NAMING_CONVENTION_CACHE =
            new StaticCache<>( new ConcatenatedLowercaseNamingConventionCacheResultProvider() );
    private final static StaticCache< String, String > CONSTANT_NAMING_CONVENTION_CACHE =
            new StaticCache<>( new ConstantNamingConventionCacheResultProvider() );
    private final static StaticCache< String, String > UNDERSCORE_NAMING_CONVENTION_CACHE =
            new StaticCache<>( new UnderscoreNamingConventionCacheResultProvider() );
    private final static StaticCache< String, String > CAMEL_CASE_NAMING_CONVENTION_CACHE =
            new StaticCache<>( new CamelCaseNamingConventionCacheResultProvider() );
}
