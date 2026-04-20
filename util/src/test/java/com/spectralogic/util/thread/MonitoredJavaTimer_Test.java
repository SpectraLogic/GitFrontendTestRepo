/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************//*

package com.spectralogic.util.thread;


import java.io.StringWriter;
import java.util.Date;
import java.util.TimerTask;

import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.apache.log4j.WriterAppender;

import com.spectralogic.util.testfrmwrk.TestUtil;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;


*/
/** *//*

public class MonitoredJavaTimer_Test 
{

    @Test
    public void testCancelProducesSpecialLogEntry()
    {
        final Logger log = org.apache.log4j.Logger.getLogger(
                                                     MonitoredJavaTimer.class );
        final String apndrName =
                         "TestAppenderFor" + MonitoredJavaTimer.class.getName();
        try
        {
            final WriterAppender apndr = new WriterAppender();
            apndr.setName( apndrName );
            apndr.setImmediateFlush( true );
            apndr.setLayout( ((Appender)Logger.getRootLogger()
                                              .getAllAppenders()
                                              .nextElement() ).getLayout() );
            final StringWriter sw = new StringWriter();
            apndr.setWriter( sw );
            
            log.addAppender( apndr );
            
            final MonitoredJavaTimer mjt = new MonitoredJavaTimer();
            mjt.cancel();
            assertTrue( "Log message shoulda contained specific cancel text.",
                        sw.toString().contains(
                        "It is dead and none of its managed tasks will run" ) );
        }
        finally
        {
            if ( null != log )
            {
                log.removeAppender( apndrName );
            }
        }
    }
    
    
    @Test
    public void testWorkerThreadDeathForLongFixedRateTasksCausesSpecialLogMessage()
    {
        final Logger log = org.apache.log4j.Logger.getLogger(
                                                     MonitoredJavaTimer.class );
        final String apndrName =
                         "TestAppenderFor" + MonitoredJavaTimer.class.getName();
        try
        {
            final WriterAppender apndr = new WriterAppender();
            apndr.setName( apndrName );
            apndr.setImmediateFlush( true );
            apndr.setLayout( ((Appender)Logger.getRootLogger()
                                              .getAllAppenders()
                                              .nextElement() ).getLayout() );
            final StringWriter sw = new StringWriter();
            apndr.setWriter( sw );
            
            log.addAppender( apndr );
            
            final MonitoredJavaTimer mjt = new MonitoredJavaTimer();
            
            mjt.scheduleAtFixedRate( new TimerTask()
                {
                    @Override
                    public void run()
                    {
                        throw new RuntimeException(
                                         "Killing Java Timer's work thread." );
                    }
                }, 0L, 100L ); // Test long delay version.
                
            int i = 40;
            while ( 0 < i-- &&
                    ! sw.toString().contains(
                              "experienced an uncaught exception. Its work" ) )
            {
                TestUtil.sleep( 50 );
            }
            assertTrue( "Log message shoulda contained specific cancel text.",
                        sw.toString().contains(
                              "experienced an uncaught exception. Its work" ) );
            mjt.cancel();
        }
        finally
        {
            if ( null != log )
            {
                log.removeAppender( apndrName );
            }
        }
    }
    
    
    @Test
    public void testWorkerThreadDeathForDateFixedRateTasksCausesSpecialLogMessage()
    {
        
        final Logger log = org.apache.log4j.Logger.getLogger(
                                                     MonitoredJavaTimer.class );
        final String apndrName =
                         "TestAppenderFor" + MonitoredJavaTimer.class.getName();
        try
        {
            final WriterAppender apndr = new WriterAppender();
            apndr.setName( apndrName );
            apndr.setImmediateFlush( true );
            apndr.setLayout( ((Appender)Logger.getRootLogger()
                                              .getAllAppenders()
                                              .nextElement() ).getLayout() );
            final StringWriter sw = new StringWriter();
            apndr.setWriter( sw );
            
            log.addAppender( apndr );
            
            final MonitoredJavaTimer mjt = new MonitoredJavaTimer();
            
            mjt.scheduleAtFixedRate( new TimerTask()
                {
                    @Override
                    public void run()
                    {
                        throw new RuntimeException(
                                         "Killing Java Timer's work thread." );
                    }
                }, new Date(), 100L ); // Test Date first run version.
                
            int i = 40;
            while ( 0 < i-- &&
                    ! sw.toString().contains(
                              "experienced an uncaught exception. Its work" ) )
            {
                TestUtil.sleep( 50 );
            }
            assertTrue( "Log message shoulda contained specific cancel text.",
                        sw.toString().contains(
                              "experienced an uncaught exception. Its work" ) );
            mjt.cancel();
        }
        finally
        {
            if ( null != log )
            {
                log.removeAppender( apndrName );
            }
        }
    }
}
*/
