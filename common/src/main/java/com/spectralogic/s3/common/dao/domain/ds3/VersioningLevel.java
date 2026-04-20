/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

public enum VersioningLevel
{
    /**
     * In order to upload a new version of an object, the previous version must first be deleted.  Note that
     * this creates a window of vulnerability where neither the old version nor new version of an object is
     * persisted.  Note also however that this provides best protection against accidental deletion of an
     * object in the case where an object is being replaced with a newer version since the replacement must
     * be done in an explicit manner with a delete of the old first.
     */
    NONE( true ),
    
    
    /**
     * When uploading a new version of an object, the previous version will be retained until the new version
     * is completely uploaded and persisted to all of its destination persistence stores.  <br><br>
     * 
     * The existing object will continue to be the authoritative version until the new object is fully
     * uploaded to cache. If the job is cancelled after the new object is fully uploaded, but before it is
     * fully persisted, the authoritative verison will be reverted to the old version.<br><br>
     * 
     * If the new version of the object is deleted prior to being completely uploaded, then the previous 
     * version will be deleted as well.
     */
    KEEP_LATEST( false ),
    /**
     * Same as keep latest, except when we are finished persisting a new verison to tape, we will delete old versions
     * only if allowed by our version lifecycle policy.
     */
    KEEP_MULTIPLE_VERSIONS( false )
    ;
    
    
    private VersioningLevel( final boolean ltfsObjectNamingAllowed )
    {
        m_ltfsObjectNamingAllowed = ltfsObjectNamingAllowed;
    }
    
    
    public boolean isLtfsObjectNamingAllowed()
    {
        return m_ltfsObjectNamingAllowed;
    }
    
    
    private final boolean m_ltfsObjectNamingAllowed;
}
