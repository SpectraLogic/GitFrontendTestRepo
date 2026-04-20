/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.reqhandler.frmwk;

import java.util.Collection;
import java.util.List;

import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.query.Query;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.iterate.EnhancedIterable;

public class FillResultSetUtil
{
    public static < T extends SimpleBeanSafeToProxy & Identifiable > int fillResultSet(
            final List< WhereClause > filters, final BeansRetriever< T > retriever,
            final RequestPagingProperties pagingProperties, final RequestSortingProperties sortingProperties,
            final Collection< T > resultSet )
    {
        if ( null == pagingProperties.getPageStartMarker() )
        {
            return fillResultSetByQuery( filters, retriever, pagingProperties, sortingProperties, resultSet );
        }
        else
        {
            return fillResultSetFromMarker( filters, retriever, pagingProperties, sortingProperties, resultSet );
        }
    }
    
    
    private static < T extends SimpleBeanSafeToProxy & Identifiable > int fillResultSetByQuery(
            final List< WhereClause > filters, final BeansRetriever< T > retriever,
            final RequestPagingProperties pagingProperties, final RequestSortingProperties sortingProperties,
            final Collection< T > resultSet )
    {
        int remainingRecordCount;
        remainingRecordCount = pagingProperties.isLastPage() ? 0 : retriever.getCount(
                Query.where( Require.all( filters ) )
                     .orderByNone()
                     .offset( pagingProperties.getPageOffset() ) );
        resultSet.addAll( retriever.retrieveAll( Query.where( Require.all( filters ) )
                                                      .orderBy( sortingProperties.getBeanSQLOrdering() )
                                                      .limit( pagingProperties.getPageLength() )
                                                      .offset( pagingProperties.getPageOffset() ) )
                                   .toList() );
        remainingRecordCount -= resultSet.size();
        if ( remainingRecordCount < 0 )
        {
            remainingRecordCount = 0;
        }
        return remainingRecordCount;
    }
    
    
    private static < T extends SimpleBeanSafeToProxy & Identifiable > int fillResultSetFromMarker(
            final List< WhereClause > filters, final BeansRetriever< T > retriever,
            final RequestPagingProperties pagingProperties, final RequestSortingProperties sortingProperties,
            final Collection< T > resultSet )
    {
        try( final EnhancedIterable< T > iter = retriever.retrieveIterable( Query.where( Require.all( filters ) )
                                                                      .orderBy(
                                                                              sortingProperties.getBeanSQLOrdering()
                                                                      ) ) )
        {
            boolean foundMarker = false;
            int recordsSkipped = 0;

            for ( T bean : iter )
            {
                if ( foundMarker )
                {
                    if ( resultSet.size() < pagingProperties.getPageLength() )
                    {
                        resultSet.add( bean );
                    }
                    else
                    {
                        break;
                    }
                }
                if ( !foundMarker )
                {
                    ++recordsSkipped;
                    if ( bean.getId()
                             .equals( pagingProperties.getPageStartMarker() ) )
                    {
                        foundMarker = true;
                    }
                }
            }

            if ( !foundMarker )
            {
                throw new S3RestException( GenericFailure.NOT_FOUND,
                        "Did not find " + RequestParameterType.PAGE_START_MARKER + " " +
                                pagingProperties.getPageStartMarker() + "." );
            }

            final int truncated = pagingProperties.getTotalRecordCount() - recordsSkipped - resultSet.size();
            return truncated < 0 ? 0 : truncated;
        }
    }
}
