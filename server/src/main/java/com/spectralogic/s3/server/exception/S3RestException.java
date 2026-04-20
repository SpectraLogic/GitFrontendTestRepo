package com.spectralogic.s3.server.exception;

import com.spectralogic.util.exception.FailureType;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.RetryObservable;

/**
 * An exception that occurs normally in the processing of S3 requests, or as a result of client
 * error using the S3 service.  For example, if the client requests a bucket that doesn't exist,
 * an S3RestException should be thrown.
 * 
 * <br><br>
 * <font color = red><b>
 * Only throw this type of exception if </font> the exception is either (i) normal or (ii) indicates
 * client error.  If you are throwing an exception due to some exceptional internal error,
 * throw a standard Java RuntimeException (or one of its subclasses) instead.
 * </b>
 * If you throw this exception class, the stack trace should not be logged.  If you throw a
 * standard Java RuntimeException, the stack trace will be logged.  This way, we don't clutter
 * up the logs with stack traces of exceptions that aren't exceptional.
 *
 */
public class S3RestException extends FailureTypeObservableException implements RetryObservable
{
    public S3RestException( final FailureType failureType, final String message )
    {
        super( failureType, message );
    }
    
    public S3RestException( final String message, final Exception e )
    {
        super( message, e );
    }

    public S3RestException( final FailureType failureType, final String message, final Exception e )
    {
        super( failureType, message, e );
    }
    
    
    public S3RestException( final Exception cause )
    {
        super( cause );
    }
    
    
    public S3RestException setRetryAfter( final int seconds )
    {
        m_retryAfterInSeconds = seconds;
        return this;
    }

    
    public int getRetryAfterInSeconds()
    {
        return m_retryAfterInSeconds;
    }
    
    
    private int m_retryAfterInSeconds = 0;
}