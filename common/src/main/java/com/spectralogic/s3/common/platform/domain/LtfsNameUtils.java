/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.domain;

import java.nio.charset.Charset;

public class LtfsNameUtils
{
    private LtfsNameUtils()
    {
        // singleton
    }
    
    
    public static String getLtfsValidationErrorMessage( final String objectName )
    {
        if ( objectName.getBytes( Charset.forName( "UTF-8" ) ).length > MAX_UTF8_ENCODED_BYTE_COUNT )
        {
            return "Object name was longer than S3 allows.";
        }
        
        final int slashSlashIndex = objectName.indexOf( "//");
    	if (0 <= slashSlashIndex)
    	{
    		return "Multiple consecutive slash chars (//) not allowed in LTFS mode.";
    	}
        
        int startIndex = 0;
        while ( true )
        {
            final int slashIndex = objectName.indexOf( '/', startIndex );
            if ( 0 > slashIndex )
            {
                return null;
            }
            if ( slashIndex - startIndex > MAX_LTFS_PATH_COMPONENT_LENGTH )
            {
                return "Slash (/) delimited path component was larger than DS3 allows.";
            }
            startIndex = slashIndex + 1; 
        }
    }
    
    
    private static final int MAX_LTFS_PATH_COMPONENT_LENGTH = 255;
    private static final int MAX_UTF8_ENCODED_BYTE_COUNT = 1024;
}
