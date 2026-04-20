/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.shared;

public enum Severity
{
    /**
     * The failure will almost certainly cause significant adverse impact in relation to the entity the
     * condition applies to.
     */
    CRITICAL,
    
    
    /**
     * The failure may cause adverse system impact in relation to the entity the condition applies to.
     */
    WARNING,
    
    
    /**
     * The failure is part of normal operation, but requires some sort of user interaction, and until this
     * occurs, there may be adverse impact in relation to the entity the condition applies to.
     */
    ALERT,
    
    
    /**
     * Notifies the user of successful completion of some event.
     */
    SUCCESS,
    
    
    /**
     * Notifies the user of some piece of information that requires no action and does not fit the other
     * categories.
     */
    INFO
}
