/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner.domain;

import java.util.UUID;

public interface CreateVerifyJobParams extends BaseCreateJobParams< CreateVerifyJobParams >
{
    String BLOB_IDS = "blobIds";
    
    UUID [] getBlobIds();
    
    CreateVerifyJobParams setBlobIds( final UUID [] value );
}
