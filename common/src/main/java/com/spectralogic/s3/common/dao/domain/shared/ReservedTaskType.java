package com.spectralogic.s3.common.dao.domain.shared;

public enum ReservedTaskType {
    /**
     * Not reserved for a specific task.
     */
    ANY,


    /**
     * Reserved for maintenance tasks only.
     */
    MAINTENANCE,


    /**
     * Reserved for reads, so this drive will not perform writes.
     */
    READ,


    /**
     * Reserved for writes, so this drive will not perform reads.
     */
    WRITE,
}
