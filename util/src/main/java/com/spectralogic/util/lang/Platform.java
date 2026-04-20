/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang;

public final class Platform
{
    /** Platform-correct newline */
    public static final String NEWLINE = System.getProperty( "line.separator" );
    
    /** 
     * <font color = red>
     * Platform-specific newline to be used only when necessary (prefer NEWLINE to this) 
     * </font>
     */
    public static final String SLASH_N = "\n";
    
    /** 
     * <font color = red>
     * Platform-specific \r to be used only when necessary
     * </font>
     */
    public static final String SLASH_R = "\r";
    
    /** Platform-correct file separator (slash) character */
    public static final String FILE_SEPARATOR = System.getProperty( "file.separator" );
}
