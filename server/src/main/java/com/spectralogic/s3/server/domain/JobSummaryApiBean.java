/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.domain;

import com.spectralogic.util.marshal.CustomMarshaledName;
import com.spectralogic.util.marshal.CustomMarshaledTypeName;


@CustomMarshaledTypeName( "JobSummary" )
public interface JobSummaryApiBean extends JobApiBean
{
    String SUMMARY = "summary";

    String getSummary();

    void setSummary(final String summary);

    String DESTINATION_SUMMARIES = "destinationSummaries";

    @CustomMarshaledName(
            value = "Destination",
            collectionValue = "Destinations",
            collectionValueRenderingMode = CustomMarshaledName.CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS
    )
    DestinationSummary[] getDestinationSummaries();

    void setDestinationSummaries(final DestinationSummary[] destinationSummaries);
}
