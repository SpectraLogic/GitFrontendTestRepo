/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.marshal;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public final class DateMarshaler
{
    private DateMarshaler()
    {
        // singleton
    }
    
    
    public static String marshal( final Date date )
    {
        // S3 date pattern: 2006-02-03T16:45:09.000Z
        final StringBuilder sb = new StringBuilder();

        final SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" );
        sdf.setTimeZone( TimeZone.getTimeZone( "UTC" ) );

        sb.append( sdf.format( date ) );
        sb.replace( 10, 11, "T" );
        sb.append( "Z" );

        return sb.toString();
    }

    
    @SuppressWarnings( "deprecation" )
    public static Date unmarshal( final String marshaledDate )
    {
        final SimpleDateFormat sdf = new SimpleDateFormat(
                ( marshaledDate.length() > 20 ) ? "yyyy-MM-dd HH:mm:ss.SSS" : "yyyy-MM-dd HH:mm:ss" );
        sdf.setTimeZone( TimeZone.getTimeZone( "UTC" ) );

        final StringBuilder sb = new StringBuilder( marshaledDate );

        if ( sb.length() > 10 && sb.charAt( 10 ) == 'T' )
        {
            sb.setCharAt(10, ' ');
        }

        if ( sb.length() > 23 && sb.charAt( 23 ) == 'Z' )
        {
            sb.deleteCharAt( 23 );
        }

        Date date = sdf.parse( sb.toString(), new ParsePosition( 0 ) );
        if ( null == date )
        {
            date = new Date( marshaledDate );
        }
        
        return date;
    }
}
