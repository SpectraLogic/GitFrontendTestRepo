/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.log;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.spectralogic.util.lang.Platform;

public final class LogUtil
{
    private LogUtil()
    {
        // singleton
    }
    
    
    /**
     * @return message header block, to call out the message in logs<br>
     *         Message will be spaced out in the logs with leading and trailing newlines
     */
    public static String getLogMessageCriticalBlock( final String message, final int numberOfCalloutLines )
    {
        return getLogMessageBlock( message, '!', true, numberOfCalloutLines, null );
    }
    
    
    /**
     * @return message header block, to call out the message in logs<br>
     *         Message will contain a leading newline, but no trailing newline
     */
    public static String getLogMessageHeaderBlock( final String message )
    {
        return getLogMessageBlock( message, '*', false, 1, null );
    }
    
    
    /**
     * @return message header block, to call out the message in logs<br>
     *         Message will contain a leading newline, but no trailing newline
     */
    public static String getAlternateLogMessageHeaderBlock(
            final char character, 
            final String message,
            final String trailingBlockText )
    {
        return getLogMessageBlock( message, character, false, 1, trailingBlockText );
    }
    

    /**
     * @return a trailer line of equal length to the banner lines
     */
    public static String getTrailerLine( String headerBlock )
    {
        while ( headerBlock.startsWith( Platform.NEWLINE ) )
        {
            headerBlock = headerBlock.substring( 1 );
        }
        
        final int index = headerBlock.indexOf( Platform.NEWLINE );
        return headerBlock.substring( 0, index );
    }
    
    
    /**
     * @return message header block, to call out the message in logs<br>
     *         Message will be spaced out in the logs with leading and trailing newlines
     */
    public static String getLogMessageImportantHeaderBlock( final String message )
    {
        return getLogMessageBlock( message, '#', true, 1, null );
    }
    
    
    private static String getLogMessageBlock(
            String message,
            final char calloutChar, 
            final boolean spaceOutInLogs,
            int numberOfCalloutLines,
            final String trailingBlockText )
    {
        while ( 70 > message.length() )
        {
            message = "   " + message + "   ";
        }
        message = calloutChar + "   " + message + "   " + calloutChar;
        if ( null != trailingBlockText )
        {
            message = message.substring( 0, message.length() - 6 - trailingBlockText.length() ) 
                      + calloutChar + "  " + trailingBlockText + "  " + calloutChar;
        }
        final StringBuilder astricksLine = new StringBuilder( message.length() );
        for ( int i = 0; i < message.length(); ++i )
        {
            astricksLine.append( calloutChar );
        }
        
        final StringBuilder retval = new StringBuilder();
        retval.append( Platform.NEWLINE );
        if ( spaceOutInLogs )
        {
            retval.append( Platform.NEWLINE );
        }
        for ( int i = 0; i < numberOfCalloutLines; ++i )
        {
            retval.append( astricksLine.toString() );
            retval.append( Platform.NEWLINE );
        }
        retval.append( message );
        for ( int i = 0; i < numberOfCalloutLines; ++i )
        {
            retval.append( Platform.NEWLINE );
            retval.append( astricksLine.toString() );
        }
        if ( spaceOutInLogs )
        {
            retval.append( Platform.NEWLINE );
            retval.append( Platform.NEWLINE );
        }
        return retval.toString();
    }
    
    
    public static String getShortVersion( final String fullText )
    {
        if ( null == fullText || 50000 > fullText.length() )
        {
            return fullText;
        }
        
        return fullText.substring( 0, 200 ) 
               + Platform.NEWLINE 
               + "." 
               + Platform.NEWLINE 
               + "." 
               + Platform.NEWLINE 
               + "(" + ( fullText.length() - 400 ) + " more characters)"
               + Platform.NEWLINE 
               + "." 
               + Platform.NEWLINE 
               + "." 
               + Platform.NEWLINE  
               + fullText.substring( fullText.length() - 200 );
    }
    
    
    public static String getShortVersion(
            final Collection< ? > collection,
            final int maxElementsToIncludeElements )
    {
        if ( null == collection )
        {
            return null;
        }
        
        if ( maxElementsToIncludeElements < collection.size() || collection.isEmpty() )
        {
            return String.valueOf( collection.size() );
        }
        return collection.size() + " (" + collection + ")";
    }
    
    
    public static String hideSecretsInUrl( final String url )
    {
        String safeString = url;
        for (String secretType : SECRETS)
        {
            safeString =safeString
                    .replaceAll( secretType + "=.*&", secretType + "=" + CONCEALED + "&" );
            safeString =safeString
                    .replaceAll( secretType + "=.*", secretType + "=" + CONCEALED + "" );
        }
        return safeString;
    }
    
    
    public static String hideSecretsInJson( final String json )
    {
        String safeString = json;
        for (String secretType : SECRETS)
        {
            safeString =safeString.replaceAll(
                    "\"" + secretType + "\":.*",
                    "\"" + secretType + "\": " + CONCEALED + "," );    
        }
        return safeString;
    }
    
    
    public final static Set<String> SECRETS = new HashSet<>();
    static
    {
        SECRETS.add( "accountKey" );
        SECRETS.add( "adminSecretKey" );
        SECRETS.add( "secretKey" );
        SECRETS.add( "account_key" );
        SECRETS.add( "admin_secret_key" );
        SECRETS.add( "secret_key" );
    }
    public final static String CONCEALED = "{CONCEALED}";    
}
