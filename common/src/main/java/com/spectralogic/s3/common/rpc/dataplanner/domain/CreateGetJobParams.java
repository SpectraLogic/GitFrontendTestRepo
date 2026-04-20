/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.dataplanner.domain;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.JobChunkClientProcessingOrderGuarantee;
import com.spectralogic.s3.common.dao.domain.ds3.IomType;
import com.spectralogic.util.bean.lang.DefaultEnumValue;
import com.spectralogic.util.bean.lang.Optional;

public interface CreateGetJobParams extends CreateVerifyJobParams
{
    String REPLICATED_JOB_ID = "replicatedJobId";
    
    @Optional
    UUID getReplicatedJobId();
    
    CreateGetJobParams setReplicatedJobId( final UUID value );


    String IOM_TYPE = "iomType";

    @DefaultEnumValue( "NONE" )
    IomType getIomType();

    CreateGetJobParams setIomType(final IomType value );
    
    
    String CHUNK_ORDER_GUARANTEE = "chunkOrderGuarantee";
    
    @DefaultEnumValue( "NONE" )
    JobChunkClientProcessingOrderGuarantee getChunkOrderGuarantee();
    
    CreateGetJobParams setChunkOrderGuarantee( final JobChunkClientProcessingOrderGuarantee value );
    
    
    CreateGetJobParams setUserId( final UUID value );
    
    CreateGetJobParams setPriority( final BlobStoreTaskPriority value );
    
    CreateGetJobParams setAggregating( final boolean value );
    
    CreateGetJobParams setDeadJobCleanupAllowed( final boolean value );
    
    CreateGetJobParams setNaked( final boolean value );
    
    CreateGetJobParams setImplicitJobIdResolution( final boolean value );
    
    CreateGetJobParams setBlobIds( final UUID [] value );
    
    CreateGetJobParams setName( final String value );

    CreateGetJobParams setProtected( final boolean value);
}
