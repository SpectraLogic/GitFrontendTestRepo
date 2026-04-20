/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.JobCreationFailed;
import com.spectralogic.s3.common.dao.domain.ds3.JobCreationFailedType;
import com.spectralogic.s3.common.dao.service.shared.ActiveFailures;
import com.spectralogic.s3.common.dao.service.shared.FailureService;

import java.util.*;
import java.util.stream.Collectors;

public interface JobCreationFailedService extends FailureService<JobCreationFailed>
{


    void create(
            final String userName,
            final JobCreationFailedType type,
            final List<List<String>> tapeBarCodes,
            final String error,
            final Integer minMinutesSinceLastFailureOfSameType );
    
    
    void deleteAll( final String userName, final JobCreationFailedType type );
    
    
    void deleteAll( final String userName );
    
    
    ActiveFailures startActiveFailures( final String userName, final JobCreationFailedType type );


    static String barCodesAsString(final List<List<String>> setOfBarCodes) {
        return setOfBarCodes.stream()
                .map(barCodes -> {
                    Collections.sort(barCodes);
                    return "{" + String.join(",", barCodes) + "}";
                })
                .collect(Collectors.joining("|"));
    }


    static List<List<String>> parseBarCodes(final String str) {
        final String[] strings = str.split("\\|");
        final List<List<String>> sets = Arrays.stream(strings)
                .map(x -> Arrays.stream(x.split(","))
                        .collect(Collectors.toList()))
                .collect(Collectors.toList());
        return sets;
    }
}
