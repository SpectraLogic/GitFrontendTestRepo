/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

public enum LtfsFileNamingMode
{
    /**
     * The LTFS file structure for OBJECT_NAME is: <br><br>
     * 
     *      {bucket name}/{object name}  <br><br>
     *      
     * The OBJECT_NAME file naming mode is appropriate when a user wants to be able to eject tapes from 
     * Bluestorm, load them up into a non-BP tape partition, and be able to see the file names match up with 
     * the object names.
     */
    OBJECT_NAME,
    
    
    /**
     * The LTFS file structure for OBJECT_ID is: <br><br>
     * 
     *      {bucket name}/{object id}  <br><br>
     *      
     * The OBJECT_ID file naming mode is appropriate when a user wants to have complete flexibility over 
     * object naming.  For example, in the OBJECT_ID file naming mode, we support object names of any length, 
     * and support object names that contain special characters like colons (":") that are prohibited by 
     * LTFS.  <br><br>
     *
     * Note that the data written to tape in OBJECT_ID file naming mode is LTFS compatible, and the object 
     * names are LTFS extended attributes, and thus, there is no reason a non-BP client can't reconstruct 
     * everything from this LTFS file naming mode.  <br><br>
     * 
     * Any bucket that uses a data policy that targets one or more storage domains with OBJECT_NAME file
     * naming mode is subject to full LTFS file naming compliance when validating object names being
     * created in said bucket.  Furthermore, no flavor of versioning (including safe replace) is supported
     * using the OBJECT_NAME file naming mode.  In order to achieve the object naming flexibility offered by 
     * the OBJECT_ID file naming mode, a bucket must use a data policy that only targets storage domains 
     * using OBJECT_ID.  <br><br>
     * 
     * Once a bucket has objects written into it that violate the LTFS file naming compliance, the data
     * policy that bucket uses is marked as requiring OBJECT_ID LTFS file naming storage domains
     * exclusively.  This is never "un-marked".
     */
    OBJECT_ID
}
