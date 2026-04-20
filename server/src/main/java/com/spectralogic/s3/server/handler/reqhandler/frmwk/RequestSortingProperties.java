/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.reqhandler.frmwk;

import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.api.RequestParameterValue;
import com.spectralogic.util.bean.BeanSQLOrdering;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.NamingConventionType;

public class RequestSortingProperties
{
    public RequestSortingProperties()
    {
    }
    
    
    RequestSortingProperties( final DS3Request request )
    {
        if ( request.hasRequestParameter( RequestParameterType.SORT_BY ) )
        {
            RequestParameterValue sortBy = request.getRequestParameter( RequestParameterType.SORT_BY );
            if ( 0 == sortBy.getString()
                            .length() )
            {
                throw new S3RestException( GenericFailure.BAD_REQUEST,
                        "Empty " + RequestParameterType.SORT_BY + " value not allowed." );
            }
            SortBy.Direction direction = SortBy.Direction.ASCENDING;
            String column = sortBy.getString();

            if ( column.startsWith( "-" ) )
            {
                direction = SortBy.Direction.DESCENDING;
                column = column.substring( 1 );
            }

            column = NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE.convert(column);
            beanSQLOrdering.add( column, direction );
        }
    }
    
    
    public BeanSQLOrdering getBeanSQLOrdering()
    {
        return beanSQLOrdering;
    }
    
    
    private final BeanSQLOrdering beanSQLOrdering = new BeanSQLOrdering();
}
