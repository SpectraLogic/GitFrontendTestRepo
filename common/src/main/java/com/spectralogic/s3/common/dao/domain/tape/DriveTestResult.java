package com.spectralogic.s3.common.dao.domain.tape;

public enum DriveTestResult {
    FAILED,
    FAILED_ALL_WRITES_TOO_SLOW,
    FAILED_FORWARD_WRITES_TOO_SLOW,
    FAILED_REVERSE_WRITES_TOO_SLOW,
    SUCCESS,
}
