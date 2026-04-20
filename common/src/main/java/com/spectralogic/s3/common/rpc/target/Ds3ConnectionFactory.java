/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc.target;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;

public interface Ds3ConnectionFactory
{
    /**
     * The admin credentials on the specified target ({@link Ds3Target#getAdminAuthId} and 
     * {@link Ds3Target#getAdminSecretKey}) will be used to establish a connection.  <br><br>
     * 
     * The specified target may be updated as necessary based on what is required to establish the connection
     * (for example, if a user id and not an authorization id is supplied as the 
     * {@link Ds3Target#getAdminAuthId}, this will be detected and corrected automatically, with the 
     * authorization id set on the specified target).  <br><br>
     * 
     * Finally, once a connection is established, we verify that the user specified via the specified target 
     * is a member of the {@link BuiltInGroup#ADMINISTRATORS} group.
     */
    Ds3Connection discover( final Ds3Target target );
    
    
    /**
     * If the <code>userId</code> is null, a connection will be created with administrator access.  Otherwise,
     * a connection will be made using the local user's credentials for pass-through authentication, without
     * verifying the user has administrator access.  <br><br>
     * 
     * Unlike {@link #discover}, no retries will be made to connect to the target by making corrections to 
     * the target.  If we cannot connect on the first attempt, we will fail to connect.
     */
    Ds3Connection connect( final UUID userId, final Ds3Target target );
}
