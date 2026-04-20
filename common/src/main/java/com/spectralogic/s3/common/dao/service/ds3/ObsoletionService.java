package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.Obsoletion;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

public interface ObsoletionService
    extends BeansRetriever< Obsoletion >, 
    BeanCreator< Obsoletion >, 
    BeanUpdater< Obsoletion >, 
    BeanDeleter
{
    public void delete( final WhereClause filter );
}
