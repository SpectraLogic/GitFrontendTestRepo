package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.UUID;

import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;

public interface DataMigration extends DatabasePersistable
{
    String PUT_JOB_ID = "putJobId";

    @Optional
    @References( Job.class )
    @CascadeDelete( WhenReferenceIsDeleted.SET_NULL )
    UUID getPutJobId();

    DataMigration setPutJobId( final UUID value );
    
    
    String GET_JOB_ID = "getJobId";

    @Optional
    @References( Job.class )
    @CascadeDelete( WhenReferenceIsDeleted.SET_NULL )
    UUID getGetJobId();
    
    DataMigration setGetJobId( final UUID value );
    
    
    String IN_ERROR = "inError";
    
    @DefaultBooleanValue( false )
    boolean isInError();
    
    DataMigration setInError( final boolean value );    
}
