/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.request.rest;

public enum RestOperationType
{
    /*
     * Keep entries in this list alphabetically sorted.
     */
    ALLOCATE,
    CANCEL_EJECT,
    CANCEL_FORMAT,
    CANCEL_IMPORT,
    CANCEL_ONLINE,
    CANCEL_TEST,
    CANCEL_VERIFY,
    CLEAN,
    COMPACT,
    DEALLOCATE,
    DUMP,
    EJECT,
    FORMAT,
    GET_PHYSICAL_PLACEMENT,
    IMPORT,
    INSPECT,
    MARK_FOR_COMPACTION,
    ONLINE,
    PAIR_BACK,
    REGENERATE_SECRET_KEY,
    START_BULK_GET,
    START_BULK_PUT,
    START_BULK_STAGE,
    START_BULK_VERIFY,
    TEST,
    VERIFY,
    VERIFY_SAFE_TO_START_BULK_PUT,
    VERIFY_PHYSICAL_PLACEMENT,
}
