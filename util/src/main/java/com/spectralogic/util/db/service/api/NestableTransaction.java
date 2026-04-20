package com.spectralogic.util.db.service.api;

public interface NestableTransaction extends BeansServiceManager, AutoCloseable
{
    public void commitNestableTransaction();
    
    
    public void closeNestableTransaction();
    
    
    public void close();
}
