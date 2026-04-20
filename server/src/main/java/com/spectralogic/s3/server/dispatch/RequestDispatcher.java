package com.spectralogic.s3.server.dispatch;

import org.springframework.ui.ModelMap;

import com.spectralogic.s3.server.frmwrk.RequestProcessingThreadRenamer;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;


/**
 * Dispatches S3 requests to the appropriate request handlers.
 */
public interface RequestDispatcher
{
    /**
     * The S3 request will be dispatched to the appropriate handle to handle it.
     */
    public ServletResponseStrategy handleS3Request( 
            final RequestProcessingThreadRenamer threadRenamer,
            final DS3Request request, 
            final ModelMap model );
}

