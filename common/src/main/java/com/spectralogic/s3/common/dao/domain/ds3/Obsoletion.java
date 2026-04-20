package com.spectralogic.s3.common.dao.domain.ds3;

import java.util.Date;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.db.lang.DatabasePersistable;

public interface Obsoletion extends DatabasePersistable
{
    String DATE = "date";
    
    @Optional
    Date getDate();
    
    Obsoletion setDate( final Date value ); 
}
