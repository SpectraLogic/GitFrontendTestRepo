package com.spectralogic.s3.server.request;

/**
 * Provides a listener mechanism for when a request completes.  Intended to be used by servlets and other
 * request processors that complete asynchronously (after the request dispatcher returns).
 */
public interface RequestCompletedListener< P >
{
    public void requestCompleted( final P param );
}
