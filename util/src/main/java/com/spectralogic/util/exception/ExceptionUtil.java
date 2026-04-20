/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.exception;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;

import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;

public final class ExceptionUtil
{
    private ExceptionUtil()
    {
        // singleton
    }
    

    @SuppressWarnings( "unchecked" )
    public static < T extends Throwable > T getExceptionOfType(
            final Class< T > exceptionType,
            final Throwable outerException )
    {
        Throwable current = outerException;
        while ( current != null && !exceptionType.isInstance( current ) )
        {
            current = current.getCause();
        }
        return (T) current;
    }
    
    
    public static RuntimeException toRuntimeException( final Throwable t )
    {
        Validations.verifyNotNull( "Throwable", t );
        if ( RuntimeException.class.isAssignableFrom( t.getClass() ) )
        {
            return (RuntimeException)t;
        }
        return new RuntimeException( t );
    }
    
    
    public static String getFullMessage( final Throwable t )
    {
        Validations.verifyNotNull( "Throwable", t );
        
        final Throwable cause = t.getCause();
        if ( null == cause )
        {
            return t.getMessage();
        }
        return t.getMessage() + Platform.NEWLINE + " --> Caused by: " + getFullMessage( cause );
    }
    
    
    public static String getFullStackTrace( final Throwable t )
    {
        final StringWriter sw = new StringWriter();
        try
        {
            METHOD_PRINT_STACK_TRACE.invoke( t, new PrintWriter( sw ) );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
        return sw.toString();
    }
    
    
    public static String getMessageWithSingleLineStackTrace( final String message, final Throwable t )
    {
        final StringBuilder retval = new StringBuilder( message + Platform.NEWLINE );
        
        final Throwable rootCause = getRootCause( t );
        retval.append( "Exception root cause: " + rootCause.getMessage() );
        retval.append( getLimitedStackTrace( rootCause.getStackTrace(), 5 ) );
        return retval.toString();
    }
    
    
    public static String getLimitedStackTrace(
            final StackTraceElement [] stackTraceElements, 
            final int maxElements )
    {
        final StringBuilder retval = new StringBuilder();
        for ( int i = 0; i < Math.min( stackTraceElements.length, maxElements ); ++i )
        {
            retval.append( Platform.NEWLINE )
                  .append( "        at " )
                  .append( stackTraceElements[ i ].toString() );
        }
        return retval.toString();
    }
    
    
    public static String getReadableMessage( final Throwable t )
    {
        if ( IgnoreObservable.class.isAssignableFrom( t.getClass() ) 
                && ( (IgnoreObservable)t ).shouldBeIgnoredWhenGeneratingReadableFailureStackTrace() )
        {
            return ( null == t.getCause() ) ? "" : getReadableMessage( t.getCause() );
        }
        if ( null == t.getMessage() )
        {
            if ( InvocationTargetException.class.isAssignableFrom( t.getClass() )
                    || UndeclaredThrowableException.class.isAssignableFrom( t.getClass() ) )
            {
                return ( null == t.getCause() ) ? "" : getReadableMessage( t.getCause() );
            }
        }
        
        final String message;
        if ( ( FailureTypeObservable.class.isAssignableFrom( t.getClass() ) ) )
        {
            final FailureType failure = ( (FailureTypeObservable)t ).getFailureType();
            message = failure.getCode() + "[" 
                      + ( (FailureTypeObservable)t ).getFailureType().getHttpResponseCode() + "]: " 
                      + t.getMessage();
        }
        else
        {
            message = t.getClass().getSimpleName() + ": " + t.getMessage();
        }
        if ( null == t.getCause() )
        {
            return message;
        }
        return message + Platform.NEWLINE + "Caused by " + getReadableMessage( t.getCause() );
    }
    
    
    public static String getRootCauseReadableMessage( final Throwable t )
    {
        if ( null != t.getCause() )
        {
            return getRootCauseReadableMessage( t.getCause() );
        }
        
        return t.getMessage();
    }
    
    
    public interface IgnoreObservable
    {
        boolean shouldBeIgnoredWhenGeneratingReadableFailureStackTrace();
    }
    
    
    private static Throwable getRootCause( final Throwable t )
    {
        if ( null == t.getCause() )
        {
            return t;
        }
        return getRootCause( t.getCause() );
    }
    
    /* CodePro warns when calling printStackTrace directly
     * as it's considered debugging code.
     */
    private final static Method METHOD_PRINT_STACK_TRACE;
    static
    {
        try
        {
            METHOD_PRINT_STACK_TRACE = Throwable.class.getMethod( "printStackTrace", PrintWriter.class );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }
}
