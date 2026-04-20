/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.marshal;

import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class DateMarshaler_Test 
{

    @Test
    public void testMarshalDoesSo()
    {
        final Calendar calendar = Calendar.getInstance( TimeZone.getTimeZone( "UTC" ) );
        calendar.set( 2006, Calendar.FEBRUARY, 3, 16, 45, 9 );
        final Long dateInMillis = calendar.getTimeInMillis();
        final Long modifiedDateInMillis = ( dateInMillis - ( dateInMillis % 1000 ) ) + 123;
        final Date date = Date.from(Instant.ofEpochMilli(modifiedDateInMillis));
        assertEquals("2006-02-03T16:45:09.123Z", DateMarshaler.marshal( date ), "Shoulda returned the expected date format.");
    }
    
    
    @Test
    public void testUnmarshalDoesSo()
    {
        performUnmarshal( "Mon, 26 Mar 2007 19:37:58 +0000" );
        performUnmarshal( "Mon, 26 Mar 2007 19:37:58 GMT" );
        performUnmarshal( "Monday, 26-Mar-07 19:37:58 GMT" );
        performUnmarshal( "Monday, 26-Mar-07 12:37:58 MST" );
        performUnmarshal( "Monday, 26-Mar-07 12:37:58 -0700" );
        performUnmarshal( "2007-03-26T19:37:58.123Z", 123 );
    }


    private static void performUnmarshal( final String dateString)
    {
        performUnmarshal(dateString, 0);
    }

    private static void performUnmarshal( final String dateString, int expectedMilliseconds )
    {
        final Calendar result = Calendar.getInstance();
        result.setTimeZone( TimeZone.getTimeZone( "UTC" ) );
        result.setTime( DateMarshaler.unmarshal( dateString ) );
        assertEquals(expectedMilliseconds,  result.get(Calendar.MILLISECOND), "Shoulda returned the expected milliseconds.");
        assertEquals(58,  result.get(Calendar.SECOND), "Shoulda returned the expected second.");
        assertEquals(37,  result.get(Calendar.MINUTE), "Shoulda returned the expected minute.");
        assertEquals(19,  result.get(Calendar.HOUR_OF_DAY), "Shoulda returned the expected hour.");
        assertEquals(26,  result.get(Calendar.DAY_OF_MONTH), "Shoulda returned the expected day.");
        assertEquals(Calendar.MARCH,  result.get(Calendar.MONTH), "Shoulda returned the ex, boolean checkMilliseconds pected month.");
        assertEquals(2007,  result.get(Calendar.YEAR), "Shoulda returned the expected year.");
    }
}
