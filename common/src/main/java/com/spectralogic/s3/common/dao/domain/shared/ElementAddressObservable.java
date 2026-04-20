/*******************************************************************************
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates. All rights
 * reserved.
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.shared;

public interface ElementAddressObservable< T >
{
    String ELEMENT_ADDRESS = "elementAddress";

    int getElementAddress();

    T setElementAddress( final int value );
}
