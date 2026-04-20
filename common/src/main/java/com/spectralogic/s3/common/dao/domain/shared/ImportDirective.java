/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.shared;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.References;

public interface ImportDirective< T >
{
    String USER_ID = "userId";

    /**
     * @return the user id that should become owner of any newly-created buckets as a result of the import
     * process
     */
    @Optional
    @References( User.class )
    UUID getUserId();
    
    T setUserId( final UUID value );
    
    
    String DATA_POLICY_ID = "dataPolicyId";

    /**
     * @return the data policy id that should be used for any newly-created buckets as a result of the import
     */
    @Optional
    @References( DataPolicy.class )
    UUID getDataPolicyId();
    
    T setDataPolicyId( final UUID value );
}
