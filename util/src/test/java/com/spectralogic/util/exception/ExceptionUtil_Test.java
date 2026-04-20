/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.exception;

import java.io.IOException;



import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class ExceptionUtil_Test
{
    @Test
    public void testGetExceptionOfTypeWhenGivenNullReturnsNull()
    {
        assertNull(
                ExceptionUtil.getExceptionOfType( Throwable.class, null ),
                "Shoulda returned null because there was no exception in the cause chain that matched."
                 );
    }


    @Test
    public void testGetExceptionOfTypeWhenOuterExceptionMatchesReturnsOuterException()
    {
        final Throwable outerException = new RuntimeException( new Exception( "inner exception" ) );
        assertSame(
                outerException,
                ExceptionUtil.getExceptionOfType( Throwable.class, outerException ),
                "Shoulda returned the outer exception."
                 );
    }


    @Test
    public void testGetExceptionOfTypeWhenInnerExceptionMatchesReturnsInnerException()
    {
        final Throwable innerException =
                new RuntimeException( new RuntimeException( "innermost exception" ) );
        final Throwable outerException = new Exception( innerException );
        assertSame(
                innerException,
                ExceptionUtil.getExceptionOfType( RuntimeException.class, outerException ),
                "Shoulda returned the inner exception."
                 );
    }


    @Test
    public void testGetExceptionOfTypeWhenNoExceptionMatchesReturnsNull()
    {
        final Throwable outerException =
                new Exception( new RuntimeException( new RuntimeException( "innermost exception" ) ) );
        assertNull(
                ExceptionUtil.getExceptionOfType( IOException.class, outerException ),
                "Shoulda returned the outer exception."
                 );
    }
    
    
    @Test
    public void testToRuntimeExceptionReturnsPopulatedCauseWhenInputIsChecked()
    {
        final Exception checkedException = new IOException( "My Exception" );
        final RuntimeException resultException = ExceptionUtil.toRuntimeException( checkedException );
        assertNotNull( resultException,
                "Shoulda returned an exception." );
        assertSame(
                checkedException,
                resultException.getCause(),
                "Shoulda had the same Cause as what we passed in."
                 );
    }
    
    
    @Test
    public void testToRuntimeExceptionReturnsSameInstanceWhenInputIsUnchecked()
    {
        final RuntimeException uncheckedException = new UnsupportedOperationException( "My Exception" );
        final RuntimeException resultException = ExceptionUtil.toRuntimeException( uncheckedException );
        assertSame(
                uncheckedException,
                resultException,
                "Shoulda just returned the exception that it received since it was unchecked."
                 );
    }
    
    
    @Test
    public void testGetFullMessageReturnContainsAllMessages()
    {
        final String message1 = "This is message #1";
        final String message2 = "This is message #2";
        final String message3 = "This is message #3";
        final Throwable throwable = new RuntimeException(
                message1,
                new RuntimeException(
                        message2,
                        new RuntimeException(
                                message3 ) ) );
        final String fullMessage = ExceptionUtil.getFullMessage( throwable );
        assertNotNull( "Shoulda returned a non-null message.", fullMessage );
        assertTrue(
                fullMessage.contains( message1 ),
                "Result shoulda contained the first message.");
        assertTrue(
                fullMessage.contains( message2 ),
                "Result shoulda contained the second message.");
        assertTrue(
                fullMessage.contains( message3 ),
                "Result shoulda contained the third message.");
    }
    
    
    @Test
    public void testGetFullStackTraceDoesNotBlowUp()
    {
        final String trace = ExceptionUtil.getFullStackTrace( new RuntimeException( "My Exception" ) );
        assertNotNull( trace,"Shoulda returned non-null" );
        assertFalse(trace.isEmpty(), "Shoulda been a non-empty string.");
    }
    
    
    @Test
    public void testGetMessageWithSingleLineStackTraceContainsOriginalMessage()
    {
        final Throwable throwable = new RuntimeException(
                new RuntimeException(
                        new RuntimeException(
                                new RuntimeException( "My Exception" ) ) ) );
        final String message = "My message";
        final String resultMessage = ExceptionUtil.getMessageWithSingleLineStackTrace(
                message,
                throwable );
        assertNotNull( "Shoulda returned a non-null message.", resultMessage );
        assertTrue(
                resultMessage.contains( message ),
                "Shoulda contained the original messages.");
    }
    
    
    @Test
    public void testGetReadableMessageDoesSo()
    {
        final RuntimeException cause = new RuntimeException( "CAUSE" );
        final RuntimeException ex = new RuntimeException( "OOPS", cause );
        
        assertTrue(
                ExceptionUtil.getReadableMessage( ex ).contains( "OOPS" ),
                "Shoulda contained all exception messages.");
        assertTrue(
                ExceptionUtil.getReadableMessage( ex ).contains( "CAUSE" ),
                "Shoulda contained all exception messages.");
        assertFalse(
                ExceptionUtil.getReadableMessage( ex ).contains( ExceptionUtil.class.getName() ),
                "Should notta contained any stack traces.");
        
        assertFalse(
                ExceptionUtil.getReadableMessage( cause ).contains( "OOPS" ),
                "Should notta contained message not in cause tree.");
        assertTrue(
                ExceptionUtil.getReadableMessage( cause ).contains( "CAUSE" ),
                "Shoulda contained all exception messages.");
        assertFalse(
                ExceptionUtil.getReadableMessage( cause ).contains( ExceptionUtil.class.getName() ),
                "Should notta contained any stack traces.");
    }
    
    
    @Test
    public void testGetRootCauseReadableMessageDoesSo()
    {
        final RuntimeException cause = new RuntimeException( "CAUSE" );
        final RuntimeException ex = new RuntimeException( "OOPS", cause );
        
        assertFalse(
                ExceptionUtil.getRootCauseReadableMessage( ex ).contains( "OOPS" ),
                "Shoulda contained only the root exception message.");
        assertTrue(
                ExceptionUtil.getRootCauseReadableMessage( ex ).contains( "CAUSE" ),
                "Shoulda contained only the root exception message.");
        assertFalse(
                ExceptionUtil.getRootCauseReadableMessage( ex ).contains( ExceptionUtil.class.getName() ),
                "Should notta contained any stack traces.");
        
        assertFalse(
                ExceptionUtil.getRootCauseReadableMessage( cause ).contains( "OOPS" ),
                "Should notta contained message not in cause tree.");
        assertTrue(
                ExceptionUtil.getRootCauseReadableMessage( cause ).contains( "CAUSE" ),
                "Shoulda contained only the root exception message.");
        assertFalse(
                ExceptionUtil.getRootCauseReadableMessage( cause ).contains(
                                ExceptionUtil.class.getName() ),
                "Should notta contained any stack traces.");
    }
}
