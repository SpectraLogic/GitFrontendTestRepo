/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.security;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;

import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Validations;

/**
 * Generates secure, random passwords.
 */
public final class PasswordGenerator
{
    public static String generate( int passwordLength )
    {
        if ( 0 > passwordLength )
        {
            throw new IllegalArgumentException( "Password length cannot be negative." );
        }
        if ( passwordLength < MINIMUM_LENGTH_TO_BE_STRONG )
        {
            LOG.info( "Password requested with " + passwordLength 
                      + " characters, but at least " + MINIMUM_LENGTH_TO_BE_STRONG 
                      + " characters must be used to generate a strong password.  "
                      + "Will generate the longer, strong password." );
            passwordLength = MINIMUM_LENGTH_TO_BE_STRONG;
        }
        
        final StringBuilder retval = new StringBuilder( passwordLength );
        for ( int i = 0; i < passwordLength; ++i )
        {
            retval.append( AUTO_GEN_CHARS[ RANDOM.nextInt( AUTO_GEN_CHARS.length ) ] );
        }
        return retval.toString();
    }
    
    
    public static void verify( final String password )
    {
        Validations.verifyNotNull( "Password", password );
        if ( password.length() < MINIMUM_LENGTH_TO_BE_STRONG )
        {
            throw new FailureTypeObservableException(
                    GenericFailure.BAD_REQUEST,
                    "Password length was " + password.length() + ", but minimum length is " 
                    + MINIMUM_LENGTH_TO_BE_STRONG + "." );
        }
        
        for ( final char c : password.toCharArray() )
        {
            if ( !ALLOWED_CHARS.contains( Character.valueOf( c ) ) )
            {
                throw new FailureTypeObservableException(
                        GenericFailure.BAD_REQUEST,
                        "Password cannot contain character '" + c 
                        + "'.  Allowed characters are: " + ALLOWED_CHARS );
            }
        }
    }
    
    
    private static List< Character > getCharacters( final char from, final char to )
    {
        final List< Character > retval = new ArrayList<>();
        for ( char c = from; c <= to; ++c )
        {
            retval.add( Character.valueOf( c ) );
        }
        
        if ( retval.isEmpty() )
        {
            throw new IllegalArgumentException( "No characters exist between " + from + " and " + to + "." );
        }
        
        return retval;
    }
    
    
    private static int getMinimumLengthToBeStrong( final char [] allowedCharacters )
    {
        for ( int i = 1; i < Integer.MAX_VALUE; ++i )
        {
            if ( Math.pow( allowedCharacters.length, i ) > REQUIRED_PERMUATIONS_TO_BE_STRONG )
            {
                return i;
            }
        }
        throw new RuntimeException( "Could not determine minimum length to be strong." );
    }
    
    
    private final static Logger LOG = Logger.getLogger( PasswordGenerator.class );
    private final static Random RANDOM = new SecureRandom();
    private final static char [] AUTO_GEN_CHARS;
    private final static Set< Character > ALLOWED_CHARS;
    private final static long REQUIRED_PERMUATIONS_TO_BE_STRONG = (long)Math.pow( 10, 12 );
    private final static int MINIMUM_LENGTH_TO_BE_STRONG;
    static
    {
        final List< Character > allowedCharacters = new ArrayList<>();
        allowedCharacters.addAll( getCharacters( 'a', 'z' ) );
        allowedCharacters.addAll( getCharacters( 'A', 'Z' ) );
        allowedCharacters.addAll( getCharacters( '0', '9' ) );
        ALLOWED_CHARS = new HashSet<>( allowedCharacters );
        
        // Remove characters that could be confused with another character
        allowedCharacters.remove( Character.valueOf( '0' ) );
        allowedCharacters.remove( Character.valueOf( 'O' ) );
        allowedCharacters.remove( Character.valueOf( 'o' ) );
        allowedCharacters.remove( Character.valueOf( '1' ) );
        allowedCharacters.remove( Character.valueOf( 'l' ) );
        
        AUTO_GEN_CHARS = new char[ allowedCharacters.size() ];
        final StringBuilder allowedCharsString = new StringBuilder();
        for ( int i = 0; i < allowedCharacters.size(); ++i )
        {
            AUTO_GEN_CHARS[ i ] = allowedCharacters.get( i ).charValue();
            allowedCharsString.append( AUTO_GEN_CHARS[ i ] );
        }
        MINIMUM_LENGTH_TO_BE_STRONG = getMinimumLengthToBeStrong( AUTO_GEN_CHARS );
        
        LOG.info( "Generated passwords may contain the following " 
                   + allowedCharsString.length() + " characters: "
                   + allowedCharsString.toString() );
        LOG.info( "Passwords must be at least " + MINIMUM_LENGTH_TO_BE_STRONG 
                   + " characters long to be strong." );
    }
}
