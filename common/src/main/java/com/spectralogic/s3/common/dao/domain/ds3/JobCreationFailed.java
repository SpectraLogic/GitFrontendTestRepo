package com.spectralogic.s3.common.dao.domain.ds3;

import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;
import com.spectralogic.util.db.lang.References;

@Indexes( @Index( JobCreationFailed.DATE ) )
//NOTE: this class is named "JobCreationFailed" instead of "JobCreationFailure" in order to work with existing
//notification registrations and the reflective code relating the constructor of BaseFailureService.java
public interface JobCreationFailed extends DatabasePersistable, Failure<JobCreationFailed, JobCreationFailedType> {

    String USER_NAME = "userName";

    String getUserName();

    JobCreationFailed setUserName(final String value );


    String TAPE_BAR_CODES = "tapeBarCodes";

    @Optional
    String getTapeBarCodes();

    JobCreationFailed setTapeBarCodes(final String value);


    JobCreationFailedType getType();

    JobCreationFailed setType(final JobCreationFailedType value );
}
