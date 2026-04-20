/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

public enum S3ObjectType
{
    DATA,
    FOLDER,
    ;
    
    
    public static S3ObjectType fromObjectName( final String objectName )
    {
        if ( objectName.endsWith( S3Object.DELIMITER ) )
        {
            return FOLDER;
        }
        return DATA;
    }
}
