/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.shared;

import com.spectralogic.util.bean.lang.Optional;

public interface ErrorMessageObservable< T >
{
    String ERROR_MESSAGE = "errorMessage";
    
    /**
     * @return null if there is no error, non-null if there is an error <br><br>
     * 
     * If there is an error message, then the entity that has the error message is considered to be in error 
     * and should not be used until the error message goes away
     */
    @Optional
    String getErrorMessage();
    
    T setErrorMessage( final String value );
}
