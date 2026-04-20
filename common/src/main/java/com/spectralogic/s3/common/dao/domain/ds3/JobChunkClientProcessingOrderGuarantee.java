/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

public enum JobChunkClientProcessingOrderGuarantee
{
    /**
     * Job chunks may be processed by the client out-of-order.
     */
    NONE,
    
    
    /**
     * Job chunks shall be processed in order by the client.
     */
    IN_ORDER
}
