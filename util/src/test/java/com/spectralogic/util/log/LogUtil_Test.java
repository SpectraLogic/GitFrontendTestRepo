/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.log;



import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class LogUtil_Test
{
    @Test
    public void testGetLogMessageCriticalBlockContainsInputMessage()
    {
        final String originalMessage = "This is the message I want to log.";
        final String logMessage = LogUtil.getLogMessageCriticalBlock( originalMessage, 2 );
        assertNotNull( "Shoulda returned non-null.", logMessage );
        assertTrue(
                logMessage.contains( originalMessage ),
                "Shoulda had the original message in the formatted one.");
    }

    
    @Test
    public void testGetLogMessageHeaderBlockContainsInputMessage()
    {
        final String originalMessage = "This is the message I want to log.";
        final String logMessage = LogUtil.getLogMessageHeaderBlock( originalMessage );
        assertNotNull( "Shoulda returned non-null.", logMessage );
        assertTrue(
                logMessage.contains( originalMessage ),
                "Shoulda had the original message in the formatted one.");
    }
    

    @Test
    public void testGetLogMessageImportantHeaderBlockContainsInputMessage()
    {
        final String originalMessage = "This is the message I want to log.";
        final String logMessage = LogUtil.getLogMessageImportantHeaderBlock( originalMessage );
        assertNotNull( "Shoulda returned non-null.", logMessage );
        assertTrue(
                logMessage.contains( originalMessage ),
                "Shoulda had the original message in the formatted one.");
    }
}
