/*
 *
 * Copyright C 2017., Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.reqhandler.frmwk;

import java.util.UUID;

import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.exception.GenericFailure;

public class RequestPagingProperties
{
    
    public RequestPagingProperties( final DS3Request request, final int totalRecordCount )
    {
        this.totalRecordCount = totalRecordCount;
        
        int pageStartSpecifiers = 0;
        lastPage = false;
        
        if ( request.hasRequestParameter( RequestParameterType.PAGE_START_MARKER ) )
        {
            pageStartMarker = request.getRequestParameter( RequestParameterType.PAGE_START_MARKER )
                                     .getUuid();
            ++pageStartSpecifiers;
        }
        
        if ( request.hasRequestParameter( RequestParameterType.PAGE_OFFSET ) )
        {
            pageOffset = request.getRequestParameter( RequestParameterType.PAGE_OFFSET )
                                .getInt();
            ++pageStartSpecifiers;
        }
        
        if ( request.hasRequestParameter( RequestParameterType.PAGE_LENGTH ) )
        {
            pageLength = request.getRequestParameter( RequestParameterType.PAGE_LENGTH )
                                .getInt();
        }
        
        if ( request.hasRequestParameter( RequestParameterType.LAST_PAGE ) )
        {
            ++pageStartSpecifiers;
            lastPage = true;
        }
        
        if ( 1 < pageStartSpecifiers )
        {
            throw new S3RestException( GenericFailure.BAD_REQUEST,
                    "To specify where the page begins, you can specify " + RequestParameterType.PAGE_OFFSET + ", or " +
                            RequestParameterType.PAGE_START_MARKER + ", or " + RequestParameterType.LAST_PAGE +
                            ", but no more than one of these at the same time." );
        }
        
        if ( ( null != pageLength ) && ( MAX_PAGE_LENGTH < pageLength ) )
        {
            throw new S3RestException( GenericFailure.BAD_REQUEST,
                    "Maximum page length that may be requested is " + MAX_PAGE_LENGTH + ", but " + pageLength +
                            " was requested." );
        }
    
        if ( null == pageLength )
        {
            pageLength = MAX_PAGE_LENGTH;
        }
        
        if ( lastPage )
        {
            pageOffset = ( totalRecordCount - getPageLength() ) <= 0 ? 0 : totalRecordCount - getPageLength();
        }
        else if ( null == pageOffset )
        {
            pageOffset = 0;
        }
    }
    
    
    UUID getPageStartMarker()
    {
        return pageStartMarker;
    }
    
    
    int getPageOffset()
    {
        return pageOffset;
    }
    
    
    int getPageLength()
    {
        return pageLength;
    }
    
    
    int getTotalRecordCount()
    {
        return totalRecordCount;
    }
    
    
    boolean isLastPage()
    {
        return lastPage;
    }
    
    
    private static final int MAX_PAGE_LENGTH = 1000;
    private UUID pageStartMarker = null;
    private Integer pageOffset = null;
    private Integer pageLength = null;
    private boolean lastPage;
    private final int totalRecordCount;
}
